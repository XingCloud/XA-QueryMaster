package com.xingcluod.qm.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.xingcloud.events.XEventOperation;
import com.xingcloud.events.XEventRange;
import com.xingcloud.qm.service.PlanMerge;
import com.xingcloud.qm.utils.GraphVisualize;
import com.xingcluod.qm.utils.Utils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.util.Selections;
import org.junit.Test;
import org.apache.log4j.Logger;

import java.util.*;

public class TestPlanMerge {
  DrillConfig c = DrillConfig.create();

  public static Logger logger= Logger.getLogger(TestPlanMerge.class);
  

  @Test
  public void testPlan8() throws Exception{
    LogicalPlan plan2 = Utils.readPlan("/plans/test1.json", c);
    LogicalPlan plan1=Utils.readPlan("/plans/test2.json", c);
    List<LogicalPlan> planList=new ArrayList<>();
    for(int i=0;i<100;i++){
        //String planName="/plans/common.hour.noseg.random."+i+".json";
        //String planName="/plans/random/random-plan."+i+".json";
        String planName="/plans1/random-plan."+i+".json";
        LogicalPlan tmpPlan=Utils.readPlan(planName,c);
        planList.add(tmpPlan);
    }
    DrillConfig config=DrillConfig.create();
    Map<LogicalPlan, LogicalPlan> merged;
      //merged=PlanMerge.sortAndMerge(planList,config);
            merged=PlanMerge.sortAndMerge(Arrays.asList(plan2),config);
    //
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
  @Test
  public void testTransferPlan() throws Exception {
      LogicalPlan plan = Utils.readPlan("/filterinscan/logical.json", c);
      for(LogicalOperator lo: plan.getSortedOperators()){
          if(lo instanceof  Scan){
              JsonNode node=((Scan)lo).getSelection().getRoot().get(0).get(Selections.SELECTION_KEY_WORD_FILTER);
              LogicalExpression le=c.getMapper().readValue(node.traverse(),LogicalExpression.class);
              System.out.println(le.toString());
          }
      }
      XEventRange range = XEventOperation.getInstance().getEventRange("sof-dsk", "visit.*");
      System.out.println("connect success");

      Map<LogicalPlan, LogicalPlan> transfered = PlanMerge.transferPlan(Arrays.asList(plan), c);
      System.out.println(transfered.values().size());
      for (LogicalPlan ret : transfered.values()) {
          System.out.println(c.getMapper().writeValueAsString(ret));
      }
      Map<LogicalPlan,LogicalPlan> merged=PlanMerge.sortAndMerge(new ArrayList<LogicalPlan>(transfered.values()),c);
      for (LogicalPlan ret : merged.values()) {
          System.out.println(c.getMapper().writeValueAsString(ret));
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
