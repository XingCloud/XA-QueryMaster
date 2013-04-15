package com.xingcloud.qm.thread;

import static com.xingcloud.qm.utils.QueryMasterVariables.ESI_MAP;

import com.xingcloud.basic.concurrent.ESProvider;
import com.xingcloud.basic.concurrent.XThreadFactory;
import com.xingcloud.qm.service.QueryBroker;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: Z J Wu
 * Date: 13-4-11
 * Time: 下午6:14
 * Package: com.xingcloud.qm.thread
 */
public class BrokerESProvider extends ESProvider {
  private static final Logger LOGGER = Logger.getLogger(BrokerESProvider.class);
  private final int brokerCount;
  private static BrokerESProvider instance;

  public synchronized static BrokerESProvider getInstance() {
    if (instance == null) {
      instance = new BrokerESProvider();
    }
    return instance;
  }

  private BrokerESProvider() {
    this.esi = ESI_MAP.get("QUERY-BROKER");
    this.brokerCount = this.esi.getThreadCount();
    this.service = Executors.newFixedThreadPool(this.brokerCount, new XThreadFactory(this.esi.getName()));
  }

  public void init() {
    for (int i = 0; i < this.brokerCount; i++) {
      this.service.execute(new QueryBroker());
    }
  }

  public static ExecutorService getService() {
    return getInstance().service;
  }
}
