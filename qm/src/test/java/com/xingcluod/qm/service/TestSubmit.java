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
    LogicalPlan plan = Utils.readPlan("/plans/test1.json",c);
    QueryMaster.getInstance().submit("2ebaf357-b6ab-464b-aad9-f3e8e02d0a6b", plan);
    Thread.sleep(10000);
    LogicalPlan plan1= Utils.readPlan("/plans/common.day.noseg.json",c);
  QueryMaster.getInstance().submit("80b04ca5-ad0f-4d2a-a388-bbffc56b9749", plan1);
    Thread.sleep(200000);
  }
}
