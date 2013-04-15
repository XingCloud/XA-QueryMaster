package com.xingcloud.qm.thread;

import static com.xingcloud.qm.utils.QueryMasterVariables.ESI_MAP;

import com.xingcloud.basic.concurrent.ESProvider;
import com.xingcloud.basic.concurrent.XThreadFactory;
import com.xingcloud.qm.redis.CachePutter;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * User: Z J Wu
 * Date: 13-4-11
 * Time: 下午6:08
 * Package: com.xingcloud.qm.thread
 */
public class CachePutESProvider extends ESProvider {
  private static final Logger LOGGER = Logger.getLogger(CachePutESProvider.class);
  private static CachePutESProvider instance;
  private final int cachePutCount;

  public synchronized static CachePutESProvider getInstance() {
    if (instance == null) {
      instance = new CachePutESProvider();
    }
    return instance;
  }

  private CachePutESProvider() {
    this.esi = ESI_MAP.get("PUT-CACHE");
    this.cachePutCount = this.esi.getThreadCount();
    service = Executors.newFixedThreadPool(this.cachePutCount, new XThreadFactory(this.esi.getName()));
  }

  public void init() {
    for (int i = 0; i < this.cachePutCount; i++) {
      service.execute(new CachePutter());
    }
    service.shutdown();
  }

  public static ExecutorService getService() {
    return getInstance().service;
  }
}
