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
  

  @Test
  public void testPlan8() throws Exception{
    //for(int i=0;i<20;i++){
    LogicalPlan plan2 = Utils.readPlan("/plans/common.day.noseg.json", c);
    LogicalPlan plan = Utils.readPlan("/plans/common.day.withseg.json", c);
      /*
    LogicalPlan plan1=Utils.readPlan("/plans/common.day.noseg.json", c);
    LogicalPlan plan3=Utils.readPlan("/plans/common.day.noseg.json", c);
    LogicalPlan plan4=Utils.readPlan("/plans/groupby.prop.noseg.json",c);
    LogicalPlan plan5=Utils.readPlan("/plans/common.hour.withseg.json",c);
    LogicalPlan plan6=Utils.readPlan("/plans/groupby.event.noseg.json",c);
    */
    //LogicalPlan plan7=Utils.readPlan("/plans/common.hour.noseg.json",c);
    /*
      LogicalPlan plan8=Utils.readPlan("/plans/groupby.prop.withseg.json",c);
    */

    List<LogicalPlan> planList=new ArrayList<>();
    for(int i=0;i<20;i++){
        //String planName="/plans/common.day.noseg.random."+i+".json";
        String planName="/plans/random/random-plan."+i+".json";
        LogicalPlan tmpPlan=Utils.readPlan(planName,c);
        planList.add(tmpPlan);
    }
    DrillConfig config=DrillConfig.create();

    /*
    PlanMerge planMerge=new PlanMerge(Arrays.asList(plan));
    planMerge.splitBigScan();

    for(LogicalPlan m: planMerge.getSplitedPlans()){
       GraphVisualize.visualize(m,"splited.png");
    }
    */
    Map<LogicalPlan, LogicalPlan> merged;
      merged=PlanMerge.sortAndMerge(planList,config);
     //       merged=PlanMerge.sortAndMerge(Arrays.asList(plan,plan2),config);
    //
    Set<LogicalPlan> set = new HashSet<>();
    set.addAll(merged.values());
    int index=0;
    for (LogicalPlan m : set) {
      index++;
      System.out.println(index);
      //GraphVisualize.visualize(m, "test"+index+".png");
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
