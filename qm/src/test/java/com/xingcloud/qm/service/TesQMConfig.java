package com.xingcloud.qm.service;

import com.xingcloud.qm.service.QueryMaster;
import org.junit.Test;

public class TesQMConfig {
  @Test
  public void testQueryMasterConfig()throws Exception{
    QueryMaster.getInstance();
    System.out.println(QueryMaster.MAX_BATCHCOST);
    System.out.println(QueryMaster.MAX_BATCHMERGE);
  }
}
