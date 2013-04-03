package com.xingcloud.qm.service;

import java.util.concurrent.Callable;

import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.Logger;

import com.xingcloud.qm.remote.QueryNode;

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
        QuerySlaveProtocol proxy = node.getProxy();
        LOGGER.info("[WORKER] - Working on service - " + sql);
        return proxy.query(sql);
    }

}
