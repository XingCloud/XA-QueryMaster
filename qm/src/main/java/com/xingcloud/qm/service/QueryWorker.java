package com.xingcloud.qm.service;

import com.xingcloud.qm.remote.QueryNode;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

public class QueryWorker implements Callable<MapWritable> {
  private static final Logger LOGGER = Logger.getLogger(QueryWorker.class);

  private String sql;

  private LogicalPlan logicalPlan;

  private QueryNode node;

  public QueryWorker(String sql, QueryNode node) {
    super();
    this.sql = sql;
    this.node = node;
  }

  public QueryWorker(LogicalPlan logicalPlan, QueryNode node) {
    this.logicalPlan = logicalPlan;
    this.node = node;
  }

  @Override
  public MapWritable call() throws Exception {

    return null;
  }

}
