package com.xingcloud.qm.service;

import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.rpc.user.QueryResultBatch;

import java.util.List;

public class PlanSubmission extends QuerySubmission {
  
  String projectID;
  
  List<QueryResultBatch> rawResult;
  
  public PlanSubmission(LogicalPlan plan, String id, String projectID) {
    super(plan, id);
    this.projectID = projectID;
  }

  public void release(){
    this.plan = null;
    if(rawResult != null){
      for (QueryResultBatch batch : rawResult) {
        if (batch.hasData()) {
          batch.getData().release();
        }
      }
      rawResult.clear();
    }
  }

  public List<QueryResultBatch> getRawResult() {
    return rawResult;
  }
}
