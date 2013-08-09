package com.xingcluod.qm.utils;

import com.xingcloud.qm.service.PlanMerge;
import com.xingcloud.qm.utils.GraphVisualize;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

public class TestVisualize {
  DrillConfig c = DrillConfig.create();

  
  @Test
  public void testVisualization()throws Exception{
    LogicalPlan plan2 = readPlan("/common.segm.json");
    LogicalPlan plan = readPlan("/common.nosegm.json");
    Map<LogicalPlan, LogicalPlan> merged = PlanMerge.sortAndMerge(Arrays.asList(plan, plan2));
    for (LogicalPlan m : merged.values()) {
      GraphVisualize.visualize(m, "test.png");      
    }
  }

  private LogicalPlan readPlan(String path) throws IOException {
    return LogicalPlan.parse(c, FileUtils.getResourceAsString(path));
  }

  
}
