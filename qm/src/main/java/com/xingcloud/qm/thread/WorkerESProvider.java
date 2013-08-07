package com.xingcloud.qm.thread;

import static com.xingcloud.qm.utils.QueryMasterVariables.ESI_MAP;

import com.xingcloud.basic.concurrent.ESProvider;
import com.xingcloud.basic.concurrent.ExecutorServiceInfo;
import com.xingcloud.basic.concurrent.XThreadFactory;
import com.xingcloud.qm.remote.QueryNode;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: Z J Wu Date: 13-4-11 Time: 下午6:15 Package: com.xingcloud.qm.thread
 */
public class WorkerESProvider extends ESProvider {
  private static final Logger LOGGER = Logger.getLogger(WorkerESProvider.class);
  private static WorkerESProvider instance;

  public synchronized static WorkerESProvider getInstance() {
    if (instance == null) {
      instance = new WorkerESProvider();
    }
    return instance;
  }

  private final int workerCount;

  private WorkerESProvider() {
    ExecutorServiceInfo brokerEsi = ESI_MAP.get("QUERY-BROKER");
    int brokerCount = brokerEsi.getThreadCount();
    int nodeCount = QueryNode.NODES.size();
    this.workerCount = brokerCount * nodeCount;
    this.esi = ESI_MAP.get("QUERY-WORKER");
    service = Executors.newFixedThreadPool(this.workerCount, new XThreadFactory(esi.getName()));
  }

  public void init() {
  }

  public static ExecutorService getService() {
    return getInstance().service;
  }
}
