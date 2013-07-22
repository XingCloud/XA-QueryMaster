package com.xingcloud.qm.service;

import com.xingcloud.basic.remote.QuerySlaveProtocol;
import com.xingcloud.qm.exceptions.XRemoteQueryException;
import com.xingcloud.qm.remote.QueryNode;
import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.Logger;

import java.util.concurrent.Callable;

public class QueryWorker implements Callable<MapWritable> {
  private static final Logger LOGGER = Logger.getLogger(QueryWorker.class);

  private String sql;

  private QueryNode node;

  public QueryWorker(String sql, QueryNode node) {
    super();
    this.sql = sql;
    this.node = node;
  }

  @Override
  public MapWritable call() throws Exception {

    QuerySlaveProtocol proxy = null;
    try {
      proxy = node.getProxy();
    } catch (Exception e) {
      throw new XRemoteQueryException("Cannot get a valid query slave proxy - " + e);
    }
    long t1 = System.currentTimeMillis();
    MapWritable nodeQueryResult = proxy.query(sql);
    long t2 = System.currentTimeMillis();
    LOGGER.info("[WORKER] - " + node.getId() + "@" + node.getHost() + ":" + node
      .getPort() + " done in " + (t2 - t1) + " milliseconds - " + sql);
    return nodeQueryResult;
  }

}
