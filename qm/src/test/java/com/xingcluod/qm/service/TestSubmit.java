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
    LogicalPlan plan = Utils.readPlan("/plans/common.day.noseg.json",c);
    QueryMaster.getInstance().submit("d401f883-72f5-4e00-853a-ac1c26d30ab3", plan);
    //QueryMaster.getInstance().submit("test2", plan2);
    Thread.sleep(200000);
  }
}
