package com.xingcloud.qm.service;

import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.rpc.user.QueryResultBatch;

import java.util.List;
import java.util.Map;

public class PlanSubmission extends QuerySubmission {
  
  String projectID;
  
  
  public Map<String, Map<String, Number[]>> values;

  public PlanSubmission(LogicalPlan plan, String id, String projectID) {
    super(plan, id);
    this.projectID = projectID;
  }

  public void release(){
    this.plan = null;
  }

  public Map<String, Map<String, Number[]>> getValues() {
    return values;
  }
}
