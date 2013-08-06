package com.xingcloud.qm.service;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.proto.UserProtos;
import org.apache.drill.exec.rpc.user.QueryResultBatch;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class PlanExecutor {
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
    planExecutor.execute(new PlanRunner(projectID, plan, listener));
  }

  private class PlanRunner implements Runnable {
    private final String projectID;
    private final LogicalPlan plan;
    private final QueryListener listner;

    public PlanRunner(String projectID, LogicalPlan plan, QueryListener listener) {
      this.projectID = projectID;
      this.plan = plan;
      this.listner = listener;
    }

    @Override
    public void run() {
      DrillClient[] clients = DrillClusterInfo.getInstance().getDrillbits();
      List<Future<List<QueryResultBatch>>> futures = new ArrayList<>(clients.length);
      for (int i = 0; i < clients.length; i++) {
        DrillClient client = clients[i];
        futures.add(drillBitExecutor.submit(new DrillbitCallable(plan, client)));
      }
      
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
      client.runQuery(UserProtos.QueryType.LOGICAL, plan.toJsonString())
      return null;  //TODO method implementation
    }
  }
}
