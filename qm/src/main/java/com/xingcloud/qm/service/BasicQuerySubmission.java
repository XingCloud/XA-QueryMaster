package com.xingcloud.qm.service;

import com.xingcloud.qm.result.ResultTable;
import org.apache.drill.common.logical.LogicalPlan;

import java.util.Map;

/**
 * 一次用户提交的查询。对应一个缓存的结果项。多个用户提交的查询可能会被合并成一个PlanSubmission提交给后台drill。
 */
public class BasicQuerySubmission extends QuerySubmission {

  ResultTable value;
  
  
  public BasicQuerySubmission(LogicalPlan plan, String id) {
    super(plan, id);
  }

  @Override
  public void release() {
    value.clear();
  }
  
}
