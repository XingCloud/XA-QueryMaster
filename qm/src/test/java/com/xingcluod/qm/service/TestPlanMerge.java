package com.xingcluod.qm.service;

import com.xingcloud.qm.service.PlanMerge;
import com.xingcloud.qm.utils.GraphVisualize;
import com.xingcluod.qm.utils.Utils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TestPlanMerge {
  DrillConfig c = DrillConfig.create();
  
  @Test
  public void testIdenticalPlan() throws Exception{
    for(int i=0;i<20;i++){
    LogicalPlan plan2 = Utils.readPlan("/plans/common.day.noseg.json", c);
    LogicalPlan plan = Utils.readPlan("/plans/common.day.withseg.json", c);
    LogicalPlan plan1=Utils.readPlan("/plans/common.day.noseg.json", c);
    /*
    PlanMerge planMerge=new PlanMerge(Arrays.asList(plan));
    planMerge.splitBigScan();

    for(LogicalPlan m: planMerge.getSplitedPlans()){
       GraphVisualize.visualize(m,"splited.png");
    }
    */
    Map<LogicalPlan, LogicalPlan> merged = PlanMerge.sortAndMerge(Arrays.asList(plan, plan1,plan2));
    Set<LogicalPlan> set = new HashSet<>();
    set.addAll(merged.values());
    for (LogicalPlan m : set) {
      GraphVisualize.visualize(m, "test.png");      
    }
    }

  }
}
