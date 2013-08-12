package com.xingcluod.qm.utils;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;

import java.io.IOException;

public class Utils {  
  
  public static LogicalPlan readPlan(String path, DrillConfig c) throws IOException {
    return LogicalPlan.parse(c, FileUtils.getResourceAsString(path));
  }

  
}
