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
    //for(int i=0;i<20;i++){
    LogicalPlan plan2 = Utils.readPlan("/plans/common.day.noseg.json", c);
    LogicalPlan plan = Utils.readPlan("/plans/common.day.withseg.json", c);
    LogicalPlan plan1=Utils.readPlan("/plans/common.day.noseg.json", c);
    LogicalPlan plan3=Utils.readPlan("/plans/common.day.noseg.json", c);
    LogicalPlan plan4=Utils.readPlan("/plans/groupby.prop.noseg.json",c);
    LogicalPlan plan5=Utils.readPlan("/plans/common.hour.withseg.json",c);
    LogicalPlan plan6=Utils.readPlan("/plans/groupby.event.noseg.json",c);
    LogicalPlan plan7=Utils.readPlan("/plans/common.hour.noseg.json",c);
    LogicalPlan plan8=Utils.readPlan("/plans/groupby.prop.withseg.json",c);
    /*
    PlanMerge planMerge=new PlanMerge(Arrays.asList(plan));
    planMerge.splitBigScan();

    for(LogicalPlan m: planMerge.getSplitedPlans()){
       GraphVisualize.visualize(m,"splited.png");
    }
    */
    Map<LogicalPlan, LogicalPlan> merged =
            PlanMerge.sortAndMerge(Arrays.asList(plan, plan1,plan2,plan3,plan4,plan5
                                                ,plan6,plan7,plan8));
    Set<LogicalPlan> set = new HashSet<>();
    set.addAll(merged.values());
    int index=0;
    for (LogicalPlan m : set) {
      index++;
      GraphVisualize.visualize(m, "test"+index+".png");
    }
    }

  //}
}
