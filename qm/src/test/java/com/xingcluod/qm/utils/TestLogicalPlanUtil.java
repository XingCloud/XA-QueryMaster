package com.xingcluod.qm.utils;

import com.xingcloud.qm.service.PlanMerge;
import com.xingcloud.qm.utils.LogicalPlanUtil;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-9-9
 * Time: 下午1:23
 * To change this template use File | Settings | File Templates.
 */
public class TestLogicalPlanUtil {

  private DrillConfig conf = DrillConfig.create();

  @Test
  public void testAddUidRangeInfo() {
    try {
     // LogicalPlan plan1 = Utils.readPlan("/plans/common.day.noseg.json", conf);
      LogicalPlan plan2 = Utils.readPlan("/plans/common.day.withseg.json", conf);
      List<LogicalPlan> plans = new ArrayList<>();
      //plans.add(plan1);
      plans.add(plan2);

      Map<LogicalPlan, LogicalPlan> mergedPlanMap = PlanMerge.sortAndMerge(plans, conf);
      Collection<LogicalPlan> mergedPlans = mergedPlanMap.values();
      LogicalPlan mergedPlan = null;
      for (LogicalPlan plan : mergedPlans) {
        mergedPlan = plan;
      }

      System.out.println(mergedPlan.toJsonString(conf));
      LogicalPlanUtil.addUidRangeInfo(mergedPlan, 0, 1);
      String planStr = mergedPlan.toJsonString(conf);
      System.out.println("------------------------------------------------------");
      System.out.println(planStr);

    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (Exception e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }

  }

}
