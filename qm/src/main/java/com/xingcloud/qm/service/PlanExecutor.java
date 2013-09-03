package com.xingcloud.qm.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xingcloud.qm.config.QMConfig;
import com.xingcloud.qm.remote.QueryNode;
import com.xingcloud.qm.result.ResultRow;
import com.xingcloud.qm.result.ResultTable;
import com.xingcloud.qm.utils.GraphVisualize;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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

    @Override
    public void run() {
      long t1 = System.currentTimeMillis(), t2;
      try {
        logger.info("PlanSubmission {} executing...", submission.id);
        if (logger.isDebugEnabled()) {
          logger.debug(
            "PlanSubmission " + submission.id + " with " + submission.plan.getGraph().getAdjList().getNodeSet()
                                                                     .size() + " LOPs...");
          String svgPath = QMConfig.conf().getString(QMConfig.TEMPDIR) + File.separator + submission.id + ".svg";
          logger.debug("saving images of PlanSubmission " + new File(svgPath).getAbsolutePath() + "...");
          GraphVisualize.visualizeMX(submission.plan, svgPath);
        }
        DrillClient[] clients = QueryNode.getClients();
        List<Future<List<QueryResultBatch>>> futures = new ArrayList<>(clients.length);
        String planString;
        try {
          planString = submission.plan.toJsonString(QueryNode.LOCAL_DEFAULT_DRILL_CONFIG);
        } catch (JsonProcessingException e) {
          e.printStackTrace();
          return;
        }
//      if (logger.isDebugEnabled()) {
//        logger.debug("[PlanString]\n{}", planString);
//      }

        for (int i = 0; i < clients.length; i++) {
          DrillClient client = clients[i];
          futures.add(drillBitExecutor.submit(new DrillbitCallable2(planString, client)));
        }
        logger.info("[PLAN-SUBMISSION] - All client submit their queries.");

        List<Map<String, ResultTable>> materializedResults = new ArrayList<>();
        //收集结果。理想情况下，应该收集所有的计算结果。
        //在有drillbit计算失败的情况下，使用剩下的结果作为估计值
        int succeeded = 0;
        Exception failedCause = null;
        for (int i = 0; i < futures.size(); i++) {
          Future<List<QueryResultBatch>> future = futures.get(i);
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
            double sampleRate = 1.0 * succeeded / futures.size();
            for (Map.Entry<String, ResultTable> entry : merged.entrySet()) {
              String queryID = entry.getKey();
              ResultTable result = entry.getValue();
              for (Map.Entry<String, ResultRow> entry2 : result.entrySet()) {
                String dimensionKey = entry2.getKey();
                ResultRow v = entry2.getValue();
                v.count /= sampleRate;
                v.sum /= sampleRate;
                v.userNum /= sampleRate;
                v.sampleRate *= sampleRate;
              }

            }
          }
          submission.queryID2Table = merged;
        }
      } catch (Exception e) {
        e.printStackTrace();
        //throw e;
      } finally {
        t2 = System.currentTimeMillis();
        logger.info("[PlanExec] time use - " + (t2 - t1));
        listener.onQueryResultReceived(submission.id, submission);
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
        result = client
          .runQuery(UserProtos.QueryType.LOGICAL, plan, QMConfig.conf().getLong(QMConfig.DRILL_EXEC_TIMEOUT));
      } else {
        logger.info("[DrillbitCallable2] - Cannot connect to server.");
      }
      return result;
    }
  }
}
