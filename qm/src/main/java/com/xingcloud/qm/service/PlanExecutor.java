package com.xingcloud.qm.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xingcloud.qm.config.QMConfig;
import com.xingcloud.qm.remote.QueryNode;
import com.xingcloud.qm.result.ResultRow;
import com.xingcloud.qm.result.ResultTable;
import com.xingcloud.qm.utils.GraphVisualize;
import com.xingcloud.qm.utils.LogicalPlanUtil;
import com.xingcloud.qm.utils.PlanWriter;
import com.xingcloud.qm.utils.QueryMasterConstant;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.graph.GraphAlgos;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PlanExecutor {

  private static final Logger logger = LoggerFactory.getLogger(PlanExecutor.class);
  private static PlanExecutor instance = new PlanExecutor();

  //for PlanRunner. 
  private static ExecutorService planExecutor = new ThreadPoolExecutor(24, 24, 30, TimeUnit.MINUTES,
                                                                       new ArrayBlockingQueue<Runnable>(256), new DaemonlizedFactory("PlanExec"));

  //for drillbitRunner.
  private static ExecutorService drillBitExecutor = new ThreadPoolExecutor(24, 24, 30, TimeUnit.MINUTES,
                                                                           new ArrayBlockingQueue<Runnable>(256), new DaemonlizedFactory("DrillbitExec"));

  public static PlanExecutor getInstance() {
    return instance;
  }

  public PlanExecutor() {

  }

  public void executePlan(PlanSubmission plan, QueryListener listener) {
    planExecutor.execute(new PlanRunner(plan, listener));
  }

  private class PlanRunner implements Runnable {
    private final QueryListener listener;
    private final PlanSubmission submission;

    public PlanRunner(PlanSubmission planSubmission, QueryListener listener) {
      this.submission = planSubmission;
      this.listener = listener;
    }

    @Override
    public void run() {
      long t1 = System.currentTimeMillis(), t2;
      try {
        _run();
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("Run plan " + submission.id + " get exception! MSG: " + e.getMessage());
      }
      t2 = System.currentTimeMillis();
      logger.info("[PlanExec] time use - " + (t2 - t1));
    }

    public void _run() throws Exception {
      if (submission.needSample) {
        Map<String, List<ResultTable>> sampleRes = new HashMap<>();  //存储每轮采样结果
        Map<String, Map<String, Long>> uidNumMap = new HashMap<>(); //记录目前已查询到的uid数量
        int startBucketPos = 0;
        Set<String> eventPatterns = LogicalPlanUtil.getEventPatterns(submission);
        List<Integer> sampleList = LogicalPlanUtil.generateSapmleList(submission.projectID, eventPatterns);
        for (int i = 0; i < sampleList.size(); i++) {
          int offset = sampleList.get(i);
          queryOneTime(startBucketPos, offset);
          startBucketPos += offset;
          List<LogicalPlan> nextRoundPlan = getNextRoundPlan(sampleRes, uidNumMap,
                  i==sampleList.size()-1, startBucketPos);
          logger.info("Next round plan number: " + nextRoundPlan.size());
          try {
            //全部plan符合采样阈值
            if (nextRoundPlan.size() == 0) {
              submission.allFinish = true;
              logger.info("All sub plan query finish for " + submission.id);
              return;
            }
          } finally {
            //更新已经满足采样阈值的结果到缓存
            listener.onQueryResultReceived(submission.id, submission);
          }
          //把没有达到采样阈值的plan重新merge，准备下一轮采样提交
          Map<LogicalPlan, LogicalPlan> mergedPlanMap = PlanMerge.sortAndMerge(nextRoundPlan, DrillConfig.create());
          Collection<LogicalPlan> mergedPlans = mergedPlanMap.values();
          assert mergedPlans.size() == 1;  //应该只合并成一个plan
          LogicalPlan nextRoundMergedPlan = mergedPlans.iterator().next();
          submission.plan = nextRoundMergedPlan;
        }
      }
    }

    /**
     * 把达到uid阈值的查询结果通知更新缓存，剩下的查询重新进行plan merge并进行下一轮提交
     * @param sampleRes  目前还没达到采样阈值plan的每轮采样查询结果
     * @param lastRound  是否是最后一轮采样
     * @param buckets 已经扫过的uid桶数（一共分为256个桶）
     * @return  没有达到采样阈值需要进行下一轮查询的logical plan
     */
    public List<LogicalPlan> getNextRoundPlan(Map<String, List<ResultTable>> sampleRes,
                                              Map<String, Map<String, Long>> uidNumMap, boolean lastRound, int buckets) {
      List<LogicalPlan> nextRoundPlan = new ArrayList<>();
      Set<String> removeList = new HashSet<>();
      for (Map.Entry<String, ResultTable> entry : submission.queryID2Table.entrySet()) {
        String queryID = entry.getKey();
        ResultTable rt = entry.getValue();
        boolean needNextRound = false;
        for (Map.Entry<String, ResultRow> subEntry : rt.entrySet()) {
          String key = subEntry.getKey();
          ResultRow rr = subEntry.getValue();
          needNextRound = !lastRound && checkUidNum(rr.userNum, uidNumMap, queryID, key);
          if (needNextRound) {
            LogicalPlan plan = submission.queryIdToPlan.get(queryID);
            nextRoundPlan.add(LogicalPlanUtil.copyPlan(plan));
            //记录此次结果
            List<ResultTable> resList = sampleRes.get(queryID);
            if (resList == null) {
              resList = new ArrayList<>();
              sampleRes.put(queryID, resList);
            }
            resList.add(rt);
            //结果集中只包含满足条件可以更新缓存的查询结果
            removeList.add(queryID);
            break;
          }
        }
        if (!needNextRound) {
          //采样结果已经达到阈值
          //把query id加入到已完成id集合
          submission.finishedIDSet.add(queryID);
          List<ResultTable> sampleResFor1Qid = sampleRes.get(queryID);
          //合并采样结果
          if (sampleResFor1Qid != null) {
            for (int i=0; i<sampleResFor1Qid.size(); i++) {
              ResultTable rtTmp = sampleResFor1Qid.get(i);
              rt.add(rtTmp);
            }
          }
          //设置采样率
          double finalRate = buckets/256.0;
          rt.setSampleRate(finalRate);
          logger.info(queryID + " set sample rate to "  + finalRate);
        }
      }

      if (!lastRound) {
        for (String queryID : removeList) {
          logger.info(queryID + " isn't satisfied uid number of " + QueryMasterConstant.SAMPLING_THRESHOLD);
          submission.queryID2Table.remove(queryID);
        }

        for (String queryID : submission.queryIdToPlan.keySet()) {
          if (!submission.finishedIDSet.contains(queryID) && !removeList.contains(queryID)) {
            logger.warn("Can't receive any result from drill-bit of " + queryID);
            nextRoundPlan.add(LogicalPlanUtil.copyPlan(submission.queryIdToPlan.get(queryID)));
          }
        }
      }

      return nextRoundPlan;
    }

    /**
     * 检查当前查询到的用户数是否以满足采样阈值，同时更新已查询到的用户数
     * @param uidNum 用户数
     * @return  是否满足用户数采样阈值
     */
    private boolean checkUidNum(long uidNum, Map<String, Map<String, Long>> uidNumMap, String queryID, String key) {
      Map<String, Long> oneQueryUidMap = uidNumMap.get(queryID);
      if (oneQueryUidMap == null) {
        oneQueryUidMap = new HashMap<>();
        oneQueryUidMap.put(key, uidNum);
        return uidNum < QueryMasterConstant.SAMPLING_THRESHOLD;
      }
      Long uidNumCurrent = oneQueryUidMap.get(key);
      if (uidNumCurrent == null) {
        oneQueryUidMap.put(key, uidNum);
        return uidNum < QueryMasterConstant.SAMPLING_THRESHOLD;
      }
      uidNumCurrent += uidNum;
      oneQueryUidMap.put(key, uidNumCurrent);
      return uidNumCurrent < QueryMasterConstant.SAMPLING_THRESHOLD;
    }

    /**
     * 提交一轮采样查询
     * @param startBucketPos  起始桶的位置
     * @param offset 采多少桶
     */
    private void queryOneTime(int startBucketPos, int offset) {
      logger.info("PlanSubmission {} executing...", submission.id + " Total query id number: " + submission.queryIdToPlan.keySet().size()
              + "\tStart bucket position: " + startBucketPos + " Offset length: " + offset);
      if (logger.isDebugEnabled()) {
        logger.debug("PlanSubmission " + submission.id + " with " + submission.plan.getGraph().getAdjList().getNodeSet()
                .size() + " LOPs...");
        String svgPath = QMConfig.conf().getString(QMConfig.TEMPDIR) + File.separator + submission.id + ".svg";
        logger.debug("Image url: http://69.28.58.61/" + submission.id + ".svg");
        GraphVisualize.visualizeMX(submission.plan, svgPath);
      }
      QueryNode[] nodes = QueryNode.getNodes();
      List<Future<List<QueryResultBatch>>> futures = new ArrayList<>(nodes.length);
      String planString;
      try {
        //把采样的uid信息加入到logical plan中
        LogicalPlanUtil.addUidRangeInfo(submission.plan, startBucketPos, offset);
        //拆分UnionedSplitScan
        submission.plan = LogicalPlanUtil.splitUnionedScan(submission.plan, 4);

        planString = submission.plan.toJsonString(QueryNode.LOCAL_DEFAULT_DRILL_CONFIG);
        PlanWriter pw = null;
        boolean writePlan = QMConfig.conf().getBoolean(QMConfig.WRITE_PLAN, false);
        if (writePlan) {
          pw = new PlanWriter(System.currentTimeMillis(), DrillConfig.create());
          pw.writeUidRangePlan(planString);
        }

      } catch (JsonProcessingException e) {
        e.printStackTrace();
        return;
      } catch (IOException e) {
        e.printStackTrace();
        return;
      }

      for (int i = 0; i < nodes.length; i++) {
        QueryNode node = nodes[i];
        futures.add(drillBitExecutor.submit(new DrillbitCallable(planString, node)));
      }
      logger.info("[PLAN-SUBMISSION] - All client submit their queries.");


        List<Map<String, ResultTable>> materializedResults = new ArrayList<>();
        //收集结果。理想情况下，应该收集所有的计算结果。
        //在有drillbit计算失败的情况下，使用剩下的结果作为估计值
        //int succeeded = 0;
        Exception failedCause = null;
        for (Future<List<QueryResultBatch>> future : futures) {
          try {
            List<QueryResultBatch> batches = future.get();
            Map<String, ResultTable> ret = RecordParser.materializeRecords(batches, QueryNode.getAllocator());
            materializedResults.add(ret);
            //succeeded++;
          } catch (Exception e) {
            logger.error("plan executing error!", e.getMessage());
            e.printStackTrace();
            failedCause = e;
          }
        }

        if (failedCause != null) {
          submission.e = failedCause;
          submission.queryID2Table = null;
          QueryMaster.getInstance().clearSubmittedTag(submission);
          throw new DrillRuntimeException("Get results from drill-bit got exception... Query failure!");
        }
        Map<String, ResultTable> merged = mergeResults(materializedResults);
        submission.queryID2Table = merged;

//        if (succeeded == 0) {
//          submission.e = failedCause;
//          submission.queryID2Table = null;
//        } else {
//          logger.debug("PlanSubmission {}: {} drillbits returned results.", submission.id, succeeded);
//          Map<String, ResultTable> merged = mergeResults(materializedResults);
//          //如果有结果没有收到，清理占位并抛异常
//          if (succeeded < futures.size()) {
//            QueryMaster.getInstance().clearSubmittedTag(submission);
//            throw new DrillRuntimeException("Get results from drill-bit got exception... Query failure!");
//          }
//          submission.queryID2Table = merged;
//        }

    }

    private Map<String, ResultTable> mergeResults(List<Map<String, ResultTable>> materializedResults) {
      Map<String, ResultTable> merged = new HashMap<>();
      for (int i = 0; i < materializedResults.size(); i++) {
        Map<String, ResultTable> result = materializedResults.get(i);
        for (Map.Entry<String, ResultTable> entry : result.entrySet()) {
          String queryID = entry.getKey();
          ResultTable value = entry.getValue();
          ResultTable mergedValue = merged.get(queryID);
          if (mergedValue == null) {
            mergedValue = new ResultTable();
            merged.put(queryID, mergedValue);
          }
          for (Map.Entry<String, ResultRow> entry2 : value.entrySet()) {
            String dimensionKey = entry2.getKey();
            ResultRow entryValue = entry2.getValue();
            ResultRow mergedEntryValue = mergedValue.get(dimensionKey);
            if (mergedEntryValue == null) {
              mergedEntryValue = entryValue;
              mergedValue.put(dimensionKey, mergedEntryValue);
            } else {
              mergedEntryValue.count += entryValue.count;
              mergedEntryValue.sum += entryValue.sum;
              mergedEntryValue.userNum += entryValue.userNum;
              mergedEntryValue.sampleRate = entryValue.sampleRate;//todo better merge sample rate
            }
          }
        }
      }
      return merged;
    }
  }

  private class DrillbitCallable implements Callable<List<QueryResultBatch>> {
    private final String plan;
    private final QueryNode node;

    public DrillbitCallable(String plan, QueryNode node) {
      this.plan = plan;
      this.node = node;
    }

    @Override
    public List<QueryResultBatch> call() throws Exception {
      List<QueryResultBatch> result = null;
      DrillClient client = node.getDrillClient();
      if (client.reconnect()) {
        long t1 = System.currentTimeMillis(), t2;
        try {
          result = client
            .runQuery(UserProtos.QueryType.LOGICAL, plan, QMConfig.conf().getLong(QMConfig.DRILL_EXEC_TIMEOUT));
        } catch (Exception e) {
          throw e;
        }
        t2 = System.currentTimeMillis();
        logger.info("[PlanExec] - Single node[{}] submit query at {},receive result at {} ,cost {} . ",node.getId(),t1,t2,(t2 - t1));
      } else {
        logger.info("[DrillbitCallable2] - Cannot connect to server.");
      }
      return result;
    }
  }
  
  static class DaemonlizedFactory implements ThreadFactory{

    AtomicInteger n = new AtomicInteger(0);
    
    String prefix = null;

    DaemonlizedFactory(String prefix) {
      this.prefix = prefix;
    }

    @Override
    public Thread newThread(Runnable r) {
      Thread ret = new Thread(r, prefix + n.getAndIncrement());
      ret.setDaemon(true);
      return ret;
    }
  }

}
