package com.xingcluod.qm.service;

import com.xingcloud.qm.service.PlanMerge;
import com.xingcloud.qm.utils.GraphVisualize;
import com.xingcluod.qm.utils.Utils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.junit.Test;

import java.util.*;

public class TestPlanMerge {
  DrillConfig c = DrillConfig.create();
  
  public void doTestMerge(String ... paths) throws Exception {
    List<LogicalPlan> plans = new ArrayList<>(paths.length);
    for (int i = 0; i < paths.length; i++) {
      String path = paths[i];
      plans.add(Utils.readPlan(path, c));
    }
    Map<LogicalPlan, LogicalPlan> merged = PlanMerge.sortAndMerge(plans);
    Set<LogicalPlan> set = new HashSet<>();
    set.addAll(merged.values());
    int i=0;
    for (LogicalPlan m : set) {
      GraphVisualize.visualize(m, "test"+(i++)+".png");      
    }    
  }
  
  @Test
  public void testIdenticalPlan() throws Exception{
    doTestMerge("/plans/common.day.noseg.json", "/plans/common.day.noseg.json");
  }
  
  @Test
  public void testPlan0() throws Exception{
    doTestMerge("/plans/common.day.noseg.json", "/plans/common.day.withseg.json");
  }
  
  @Test
  public void testPlan6() throws Exception{
    doTestMerge("/plans/common.day.noseg.json", 
      "/plans/common.day.withseg.json",
      "/plans/common.hour.noseg.json",
      "/plans/common.hour.withseg.json",
      "/plans/groupby.event.noseg.json",
      "/plans/groupby.event.withseg.json",
      "/plans/groupby.prop.noseg.json",
      "/plans/groupby.prop.withseg.json"
    );
  }
  
  
  
}
