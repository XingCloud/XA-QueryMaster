package com.xingcloud.qm.utils;

import com.xingcloud.basic.concurrent.ExecutorServiceInfo;
import com.xingcloud.basic.conf.ConfigReader;
import com.xingcloud.basic.conf.Dom;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class QueryMasterVariables {
  private static final Logger LOGGER = Logger.getLogger(QueryMasterVariables.class);

  public static Map<String, ExecutorServiceInfo> ESI_MAP;
  public static boolean PUT_CACHE;
  public static boolean USING_LOCAL_FAKE_SERVER;

  static {
    Dom root = null;
    try {
      root = ConfigReader.getDom("executor_services.xml");
      Dom esd = root.element("executor-services");
      List<Dom> es = esd.elements("executor-service");
      ESI_MAP = new HashMap<String, ExecutorServiceInfo>(es.size());
      String id = null;
      String name = null;
      String threadCountString;
      int threadCount = 0;
      Dom threadCountDom = null;
      ExecutorServiceInfo tfi = null;
      for (Dom d : es) {
        id = d.getAttributeValue("id");
        name = d.elementText("name");
        threadCountDom = d.element("count");
        if (threadCountDom == null) {
          tfi = new ExecutorServiceInfo(id, name);
        } else {
          threadCountString = d.elementText("count");
          threadCount = Integer.parseInt(threadCountString);
          tfi = new ExecutorServiceInfo(id, name, threadCount);
        }
        ESI_MAP.put(id, tfi);
        LOGGER.info("[QM-SYS-VAR] - ExecutorServiceInfo - " + tfi);
      }

      root = ConfigReader.getDom("common.xml");
      String putCacheString = root.element("put-cache").getAttributeValue("enabled");
      PUT_CACHE = Boolean.valueOf(putCacheString);
      LOGGER.info("[QM-SYS-VAR] - PUT_CACHE - " + PUT_CACHE);

      String usingLocalServer = root.element("local-server").getAttributeValue("enabled");
      USING_LOCAL_FAKE_SERVER = Boolean.parseBoolean(usingLocalServer);
      LOGGER.info("[QM-SYS-VAR] - FAKE_SERVER - " + USING_LOCAL_FAKE_SERVER);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static void init() {

  }

  public static void main(String[] args) {
    Map<String, ExecutorServiceInfo> map = QueryMasterVariables.ESI_MAP;
    System.out.println(map);
  }
}
