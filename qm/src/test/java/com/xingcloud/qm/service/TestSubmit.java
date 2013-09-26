package com.xingcloud.qm.service;

import com.xingcloud.qm.service.QueryMaster;
import com.xingcloud.qm.utils.Utils;
import com.xingcloud.qm.utils.Utils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.junit.Test;

public class TestSubmit {
  DrillConfig c = DrillConfig.create();  
  
  @Test
  public void testSubmit0()throws Exception{
    LogicalPlan plan = Utils.readPlan("/plans/test2.json", c);
    QueryMaster.getInstance().submit("COMMON,sof-dsk,2013-01-01,2013-01-01,click.*.*.1,TOTAL_USER,VF-ALL-0-0,PERIOD", plan);
    Thread.sleep(10000);
      /*
    LogicalPlan plan1= Utils.readPlan("/plans/common.day.noseg.json",c);
  QueryMaster.getInstance().submit("80b04ca5-ad0f-4d2a-a388-bbffc56b9749", plan1);
    */
    Thread.sleep(200000);

  }
}
