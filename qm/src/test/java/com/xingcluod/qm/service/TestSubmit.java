package com.xingcluod.qm.service;

import com.xingcloud.qm.service.QueryMaster;
import com.xingcluod.qm.utils.Utils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.junit.Test;

public class TestSubmit {
  DrillConfig c = DrillConfig.create();  
  
  @Test
  public void testSubmit0()throws Exception{
    LogicalPlan plan2 = Utils.readPlan("/plans/common.segm.json", c);
    LogicalPlan plan = Utils.readPlan("/plans/common.nosegm.json",c);
    QueryMaster.getInstance().submit("test1", plan);
    QueryMaster.getInstance().submit("test2", plan2);
    Thread.sleep(200000);
  }
}
