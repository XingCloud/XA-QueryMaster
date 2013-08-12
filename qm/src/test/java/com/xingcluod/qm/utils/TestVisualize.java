package com.xingcluod.qm.utils;

import com.xingcloud.qm.service.PlanMerge;
import com.xingcloud.qm.utils.GraphVisualize;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.junit.Test;

import java.util.Arrays;
import java.util.Map;

public class TestVisualize {
  DrillConfig c = DrillConfig.create();

  
  @Test
  public void testVisualization()throws Exception{
    LogicalPlan plan2 = Utils.readPlan("/plans/common.segm.json", c);
    LogicalPlan plan = Utils.readPlan("/plans/common.nosegm.json", c);
    Map<LogicalPlan, LogicalPlan> merged = PlanMerge.sortAndMerge(Arrays.asList(plan, plan2));
    for (LogicalPlan m : merged.values()) {
      GraphVisualize.visualize(m, "test.png");      
    }
  }
  
}
