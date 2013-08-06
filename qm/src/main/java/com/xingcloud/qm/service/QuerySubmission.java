package com.xingcloud.qm.service;

import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.rpc.user.QueryResultBatch;

import java.util.List;

/**
 * 代表一次查询提交。
 * 客户端提交来的查询，可以是以一个cache结果ID为ID。
 * 合并后的查询，可以是以一个projectID为ID。
 */
public abstract class QuerySubmission {
  String id;
  LogicalPlan plan;
  long startTime;
  
  //简单模型，来估计plan的执行代价
  //TODO 基于cost来控制Plan的执行数量
  float cost = 1;

  public QuerySubmission(LogicalPlan plan, String id) {
    this.plan = plan;
    this.id = id;
    this.startTime = System.currentTimeMillis();
    this.cost = 1;
  }

  public long getStartTime() {
    return startTime;
  }

  public float getCost() {
    return cost;
  }

  abstract public void release();
}
