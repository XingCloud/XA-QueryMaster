package com.xingcluod.qm.service;

import com.xingcloud.qm.service.PlanMerge;
import com.xingcloud.qm.utils.GraphVisualize;
import com.xingcloud.qm.utils.Utils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class TestPlanMerge {
  DrillConfig c = DrillConfig.create();

  public static Logger logger= LoggerFactory.getLogger(TestPlanMerge.class);


  @Test
  public void testPlan8() throws Exception{
    List<LogicalPlan> planList=new ArrayList<>();
    for(int i=0;i<100;i++){
        //String planName="/plans/common.hour.noseg.random."+i+".json";
        //String planName="/plans/random/random-plan."+i+".json";
        String planName="/random_plans/random-plan."+i+".json";
        LogicalPlan tmpPlan=Utils.readPlan(planName,c);
        planList.add(tmpPlan);
    }
    DrillConfig config=DrillConfig.create();
    Map<LogicalPlan, LogicalPlan> merged;
    merged=PlanMerge.sortAndMerge(planList,config);
    Set<LogicalPlan> set = new HashSet<>();
    set.addAll(merged.values());
    int index=0;
    for (LogicalPlan m : set) {
      index++;
      String planStr=config.getMapper().writeValueAsString(m);
      System.out.println(planStr);
      LogicalPlan result=LogicalPlan.parse(config,planStr);
      GraphVisualize.visualizeMX(m, "test"+index+".svg");
    }
  }


  public void doTestMerge(String ... paths) throws Exception {
    List<LogicalPlan> plans = new ArrayList<>(paths.length);
    for (int i = 0; i < paths.length; i++) {
      String path = paths[i];
      plans.add(Utils.readPlan(path, c));
    }
    Map<LogicalPlan, LogicalPlan> merged = PlanMerge.sortAndMerge(plans,DrillConfig.create());
    Set<LogicalPlan> set = new HashSet<>();
    set.addAll(merged.values());
    int i=0;
    for (LogicalPlan m : set) {
      GraphVisualize.visualizeMX(m, "test"+(i++)+".svg");
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
