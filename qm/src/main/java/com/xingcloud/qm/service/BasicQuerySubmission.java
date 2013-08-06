package com.xingcloud.qm.service;

import org.apache.drill.common.logical.LogicalPlan;

import java.util.Map;

public class BasicQuerySubmission extends QuerySubmission {

  Map<String, Number[]> value;
  
  
  public BasicQuerySubmission(LogicalPlan plan, String id) {
    super(plan, id);
  }

  @Override
  public void release() {
    value.clear();
  }
  
  
}
