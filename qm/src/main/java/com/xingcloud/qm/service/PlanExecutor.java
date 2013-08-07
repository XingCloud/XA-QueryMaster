package com.xingcloud.qm.service;

import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class PlanExecutor {
  
  private static final Logger logger = LoggerFactory.getLogger(PlanExecutor.class);
  private static PlanExecutor instance;

  //for PlanRunner. 
  private static ExecutorService planExecutor = new ThreadPoolExecutor(
    24,24,30, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(256)
  );

  //for drillbitRunner.
  private static ExecutorService drillBitExecutor = new ThreadPoolExecutor(
    24,24,30, TimeUnit.MINUTES, new ArrayBlockingQueue<Runnable>(256)
  );
  
  public static PlanExecutor getInstance() {
    return instance;
  }

  public void executePlan(String projectID, LogicalPlan plan, QueryListener listener) {
    planExecutor.execute(new PlanRunner(new PlanSubmission(plan, projectID+"."+System.currentTimeMillis(), projectID), listener));
  }

  private class PlanRunner implements Runnable {
    private final QueryListener listner;
    private final PlanSubmission submission;


    public PlanRunner(PlanSubmission planSubmission, QueryListener listener) {
      this.submission = planSubmission;
      this.listner = listener;
    }

    @Override
    public void run() {
      DrillClient[] clients = DrillClusterInfo.getInstance().getDrillbits();
      List<Future<List<QueryResultBatch>>> futures = new ArrayList<Future<List<QueryResultBatch>>>(clients.length);
      for (int i = 0; i < clients.length; i++) {
        DrillClient client = clients[i];
        futures.add(drillBitExecutor.submit(new DrillbitCallable(submission.plan, client)));
      }
      List<Map<String, Map<String, Number[]>>> materializedResults = new ArrayList<Map<String, Map<String,Number[]>>>();
      for (int i = 0; i < futures.size(); i++) {
        Future<List<QueryResultBatch>> future = futures.get(i);
        try {
          List<QueryResultBatch> batches = future.get();
          Map<String, Map<String, Number[]>> ret = RecordParser.materializeRecords(batches, DrillClusterInfo.getInstance().getAllocator());
          materializedResults.add(ret);
        } catch (Exception e) {
          logger.warn("plan executing error", e);
          e.printStackTrace();  //e:
          return;
        }
      }
      Map<String, Map<String, Number[]>> merged = mergeResults(materializedResults);
      submission.values = merged;
      listner.onQueryResultRecieved(submission.id, submission);
    }

    private Map<String, Map<String, Number[]>> mergeResults(List<Map<String, Map<String, Number[]>>> materializedResults) {
      Map<String, Map<String, Number[]>> merged = new HashMap<String, Map<String, Number[]>>();
      for (int i = 0; i < materializedResults.size(); i++) {
        Map<String, Map<String, Number[]>> result = materializedResults.get(i);
        for (Map.Entry<String, Map<String, Number[]>> entry : result.entrySet()) {
          String queryID = entry.getKey();
          Map<String, Number[]> value = entry.getValue();
          Map<String, Number[]> mergedValue = merged.get(queryID);
          if(mergedValue == null){
            mergedValue = new HashMap<String, Number[]>();
            merged.put(queryID, mergedValue);
          }
          for (Map.Entry<String, Number[]> entry2 : value.entrySet()) {
            String dimensionKey = entry2.getKey();
            Number[] entryValue = entry2.getValue();
            Number[] mergedEntryValue = mergedValue.get(dimensionKey);
            if(mergedEntryValue == null){
              mergedEntryValue = entryValue;
              mergedValue.put(dimensionKey, mergedEntryValue);
            }else{
              for (int j = 0; j < mergedEntryValue.length; j++) {
                mergedEntryValue[j] = (Long)mergedEntryValue[j] + (Long)entryValue[j];
              }
            }
          }
        }
      }
      return merged;
    }
  }

  private class DrillbitCallable implements Callable<List<QueryResultBatch>>{
    private final LogicalPlan plan;
    private final DrillClient client;

    public DrillbitCallable(LogicalPlan plan, DrillClient client) {
      this.plan = plan;
      this.client = client;
    }

    @Override
    public List<QueryResultBatch> call() throws Exception {
      return client.runQuery(UserProtos.QueryType.LOGICAL, plan.toJsonString(DrillClusterInfo.getInstance().getLocalConfig()));
    }
  }
}
