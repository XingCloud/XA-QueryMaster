package com.xingcloud.qm.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xingcloud.qm.config.QMConfig;
import com.xingcloud.qm.remote.QueryNode;
import com.xingcloud.qm.result.ResultRow;
import com.xingcloud.qm.result.ResultTable;
import com.xingcloud.qm.utils.GraphVisualize;
import com.xingcloud.qm.utils.LogicalPlanUtil;
import com.xingcloud.qm.utils.QueryMasterConstant;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class PlanExecutor {

  private static final Logger logger = LoggerFactory.getLogger(PlanExecutor.class);
  private static PlanExecutor instance = new PlanExecutor();

  //for PlanRunner. 
  private static ExecutorService planExecutor = new ThreadPoolExecutor(24, 24, 30, TimeUnit.MINUTES,
                                                                       new ArrayBlockingQueue<Runnable>(256));

  //for drillbitRunner.
  private static ExecutorService drillBitExecutor = new ThreadPoolExecutor(24, 24, 30, TimeUnit.MINUTES,
                                                                           new ArrayBlockingQueue<Runnable>(256));

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

    public void _run() throws Exception {
      if (submission.needSample) {
        Map<String, List<ResultTable>> samplingResult = new HashMap<>();  //存储每轮采样结果
        Map<String, Map<String, Long>> uidNumMap = new HashMap<>(); //记录目前已查询到的uid数量
        int passedBucket = 0;
        for (int i = 0; i < QueryMasterConstant.SAMPLING_ARRAY.length; i++) {
          int offset = QueryMasterConstant.SAMPLING_ARRAY[i];
          queryOneTime(passedBucket, offset);
          passedBucket += offset;
          List<LogicalPlan> nextRoundPlan = getNextRoundPlan(samplingResult, uidNumMap);
          //更新已经满足采样阈值的结果到缓存
          try {
            if (nextRoundPlan.size() == 0) {
              submission.allFinish = true;
              logger.info("All sub plan query finish for " + submission.id);
              return;
            }
          } finally {
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
     * @param samplingResult  目前还没达到采样阈值plan的每轮采样查询结果
     * @return  没有达到采样阈值需要进行下一轮查询的logical plan
     */
    public List<LogicalPlan> getNextRoundPlan(Map<String, List<ResultTable>> samplingResult, Map<String, Map<String, Long>> uidNumMap) {
      Map<String, ResultTable> resultTableMap = submission.queryID2Table;
      List<LogicalPlan> nextRoundPlan = new ArrayList<>();
      for (Map.Entry<String, ResultTable> entry : resultTableMap.entrySet()) {
        String queryID = entry.getKey();
        ResultTable rt = entry.getValue();
        boolean needNextRound = false;
        for (Map.Entry<String, ResultRow> subEntry : rt.entrySet()) {
          String key = subEntry.getKey();
          ResultRow rr = subEntry.getValue();
          long uidNum = rr.userNum;
          List<ResultTable> resList = samplingResult.get(queryID);
          needNextRound = checkUidNum(uidNum, uidNumMap, queryID, key);
          if (needNextRound) {
            LogicalPlan plan = submission.queryIdToPlan.get(queryID);
            nextRoundPlan.add(plan);
            //记录此次结果
            if (resList == null) {
              resList = new ArrayList<>();
              samplingResult.put(queryID, resList);
            }
            resList.add(rt);
            //结果集中只包含满足条件可以更新缓存的查询结果
            resultTableMap.remove(queryID);
            break;
          }
        }
        if (!needNextRound) {
          //采样结果已经达到阈值
          List<ResultTable> samplingResults = samplingResult.get(queryID);
          //合并采样结果
          ResultTable rtFinal = new ResultTable();
          if (samplingResult != null) {
            for (int i=0; i<samplingResults.size(); i++) {
              ResultTable rtTmp = samplingResults.get(i);
              rtFinal.add(rtTmp, QueryMasterConstant.SAMPLING_ARRAY[i]/(double)QueryMasterConstant.TOTAL_BUCKET_NUM);
            }
            //加上此轮采样查询结果
            rtFinal.add(rt, QueryMasterConstant.SAMPLING_ARRAY[samplingResult.size()]/(double)QueryMasterConstant.TOTAL_BUCKET_NUM);
          } else {
            //第一轮采样查询就满足阈值条件
            rtFinal = rt;
          }
          //结果值除以采样率得到最终值
          Collection<ResultRow> rows = rtFinal.values();
          for (ResultRow rr : rows) {
            rr.userNum = (long) (rr.userNum/rr.sampleRate);
            rr.sum = (long) (rr.sum/rr.sampleRate);
            rr.count = (long) (rr.count/rr.sampleRate);
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
        return uidNum > QueryMasterConstant.SAMPLING_THRESHOLD;
      }
      Long uidNumCurrent = oneQueryUidMap.get(key);
      if (uidNumCurrent == null) {
        oneQueryUidMap.put(key, uidNum);
        return uidNum > QueryMasterConstant.SAMPLING_THRESHOLD;
      }
      uidNumCurrent += uidNum;
      oneQueryUidMap.put(key, uidNumCurrent);
      return uidNumCurrent > QueryMasterConstant.SAMPLING_THRESHOLD;
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

    /**
     * 提交一轮采样查询
     * @param startBucketPos  起始桶的位置
     * @param offsetLen 采多少桶
     */
    private void queryOneTime(int startBucketPos, int offsetLen) {
      logger.info("PlanSubmission {} executing...", submission.id + " Total query id number: " + submission.queryIdToPlan.keySet().size()
              + "\tStart bucket position: " + startBucketPos + " Offset length: " + offsetLen);
      if (logger.isDebugEnabled()) {
        logger.debug("PlanSubmission " + submission.id + " with " + submission.plan.getGraph().getAdjList().getNodeSet()
                .size() + " LOPs...");
        String svgPath = QMConfig.conf().getString(QMConfig.TEMPDIR) + File.separator + submission.id + ".svg";
        logger.debug("Image url: http://69.28.58.61/" + submission.id + ".svg");
        GraphVisualize.visualizeMX(submission.plan, svgPath);
      }
      DrillClient[] clients = QueryNode.getClients();
      List<Future<List<QueryResultBatch>>> futures = new ArrayList<>(clients.length);
      String planString;
      try {
        //把采样的uid信息加入到logical plan中
        LogicalPlanUtil.addUidRangeInfo(submission.plan, startBucketPos, offsetLen);
        planString = submission.plan.toJsonString(QueryNode.LOCAL_DEFAULT_DRILL_CONFIG);
      } catch (JsonProcessingException e) {
        e.printStackTrace();
        return;
      } catch (IOException e) {
        e.printStackTrace();
        return;
      }

      for (int i = 0; i < clients.length; i++) {
        DrillClient client = clients[i];
        futures.add(drillBitExecutor.submit(new DrillbitCallable2(planString, client)));
      }
      logger.info("[PLAN-SUBMISSION] - All client submit their queries.");

      try {
        List<Map<String, ResultTable>> materializedResults = new ArrayList<>();
        //收集结果。理想情况下，应该收集所有的计算结果。
        //在有drillbit计算失败的情况下，使用剩下的结果作为估计值
        int succeeded = 0;
        Exception failedCause = null;
        for (Future<List<QueryResultBatch>> future : futures) {
          try {
            List<QueryResultBatch> batches = future.get();
            Map<String, ResultTable> ret = RecordParser.materializeRecords(batches, QueryNode.getAllocator());
            materializedResults.add(ret);
            succeeded++;
          } catch (Exception e) {
            logger.warn("plan executing error", e);
            failedCause = e;
          }
        }

        if (succeeded == 0) {
          submission.e = failedCause;
          submission.queryID2Table = null;
        } else {
          logger.debug("PlanSubmission {}: {} drillbits returned results.", submission.id, succeeded);
          Map<String, ResultTable> merged = mergeResults(materializedResults);
          //如果有结果没有收到，则根据采样率估计值
          if (succeeded < futures.size()) {
            double sampleRate = 1.0 * offsetLen / QueryMasterConstant.TOTAL_BUCKET_NUM * succeeded / futures.size();
            for (Map.Entry<String, ResultTable> entry : merged.entrySet()) {
              ResultTable result = entry.getValue();
              for (Map.Entry<String, ResultRow> entry2 : result.entrySet()) {
                ResultRow v = entry2.getValue();
                v.sampleRate *= sampleRate;
              }
            }
          }
          submission.queryID2Table = merged;
        }
      } catch (Exception e) {
        e.printStackTrace();
        logger.error("Query one time get exception! MSG: " + e.getMessage());
      }
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
    private final LogicalPlan plan;
    private final DrillClient client;

    public DrillbitCallable(LogicalPlan plan, DrillClient client) {
      this.plan = plan;
      this.client = client;
    }

    @Override
    public List<QueryResultBatch> call() throws Exception {
      return client.runQuery(UserProtos.QueryType.LOGICAL, plan.toJsonString(QueryNode.LOCAL_DEFAULT_DRILL_CONFIG),
                             QMConfig.conf().getLong(QMConfig.DRILL_EXEC_TIMEOUT));
    }
  }

  private class DrillbitCallable2 implements Callable<List<QueryResultBatch>> {
    private final String plan;
    private final DrillClient client;

    public DrillbitCallable2(String plan, DrillClient client) {
      this.plan = plan;
      this.client = client;
    }

    @Override
    public List<QueryResultBatch> call() throws Exception {
      List<QueryResultBatch> result = null;

      if (client.reconnect()) {
        long t1 = System.nanoTime(), t2;
        try {
          result = client
            .runQuery(UserProtos.QueryType.LOGICAL, plan, QMConfig.conf().getLong(QMConfig.DRILL_EXEC_TIMEOUT));
        } catch (Exception e) {
          throw e;
        }
        t2 = System.nanoTime();
        logger.info("[PlanExec] - Single future get use " + (t2 - t1) / 1000000 + " milliseconds.");
      } else {
        logger.info("[DrillbitCallable2] - Cannot connect to server.");
      }
      return result;
    }
  }
}
