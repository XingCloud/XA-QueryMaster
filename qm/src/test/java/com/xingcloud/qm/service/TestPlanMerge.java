package com.xingcloud.qm.service;

import com.xingcloud.qm.utils.GraphVisualize;
import com.xingcloud.qm.utils.Utils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.junit.Test;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileWriter;
import java.io.Writer;
import java.util.*;

public class TestPlanMerge {
  DrillConfig c = DrillConfig.create();

  public static Logger logger = Logger.getLogger(TestPlanMerge.class);


  public void testPlans(List<LogicalPlan> planList, String[] outputDirs) throws Exception {
    DrillConfig config = DrillConfig.create();
    Map<LogicalPlan, LogicalPlan> merged;
    merged = PlanMerge.sortAndMerge(planList, config);
    for (int i = 0; i < outputDirs.length; i++) {
      File dir = new File(outputDirs[i]);
      //File dir = new File("/home/yb/workspace/gitdata/incubator-drill/sandbox/prototype/exec/java-exec/src/test/resources/qmplans");
      dir.mkdir();
      int index = 0;
      for (LogicalPlan ret : new HashSet<>(merged.values())) {
        String planStr = c.getMapper().writeValueAsString(ret);
        LogicalPlan result = c.getMapper().readValue(planStr, LogicalPlan.class);
        logger.info(planStr);
        String dirPath = dir.getAbsolutePath();
        if (dirPath.contains("target/test-classes"))
          dirPath = dirPath.replace("target/test-classes", "src/test/resources");
        File targetFile = new File(dir.getAbsolutePath() + "/" + (++index) + ".json");
        Writer writer = new FileWriter(targetFile);
        writer.write(planStr);
        writer.flush();
        writer.close();
        GraphVisualize.visualizeMX(ret, "test" + (++index) + ".svg");
      }
    }
  }

  public void doTestMerge(String... paths) throws Exception {
    List<LogicalPlan> plans = new ArrayList<>(paths.length);
    for (int i = 0; i < paths.length; i++) {
      String path = paths[i];
      plans.add(Utils.readPlan(path, c));
    }
    Map<LogicalPlan, LogicalPlan> merged = PlanMerge.sortAndMerge(plans, DrillConfig.create());
    Set<LogicalPlan> set = new HashSet<>();
    set.addAll(merged.values());
    int i = 0;
    for (LogicalPlan m : set) {
      GraphVisualize.visualizeMX(m, "test" + (i++) + ".svg");
    }
  }

  @Test
  public void testIdenticalPlan() throws Exception {
    doTestMerge("/plans/common.day.noseg.json", "/plans/common.day.noseg.json");
  }

  @Test
  public void testPlan0() throws Exception {
    doTestMerge("/plans/common.day.noseg.json", "/plans/common.day.withseg.json");
  }

  @Test
  public void testPlan6() throws Exception {
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

  //same plan
  @Test
  public void testCase0() throws Exception {
    String origDir = "/same_plans";
    testCase(origDir);
  }

  @Test
  public void testCase1() throws Exception {
    String origDir = "/contain_plans";
    testCase(origDir);
  }

  @Test
  public void testCase2() throws Exception {
    String origDir = "/rkcross_plans";
    testCase(origDir);
  }

  @Test
  public void testCase3() throws Exception {
    String origDir = "/sametable_plans";
    testCase(origDir);
  }

  @Test
  public void testCase4() throws Exception {
    String origDir = "/differenttable_plans";
    testCase(origDir);
  }

  @Test
  public void testCase5() throws Exception {
    String origDir="/random_plans";
    testCase(origDir);
  }

  @Test
  public void testCase6() throws Exception {
    String origDir="/allevent_plans";
    testCase(origDir);
  }


  public void testCase(String origDir) throws Exception {
    List<LogicalPlan> planList = new ArrayList<>();
    File origDirFile = FileUtils.getResourceAsFile(origDir);
    //File[] childFiles=origDirFile.listFiles();
    for (String path : origDirFile.list()) {
      if (!path.contains(origDir))
        path = origDir + "/" + path;
      planList.add(Utils.readPlan(path, c));
    }
    String outputDirs[] = {"/home/yb/workspace/gitdata/incubator-drill/sandbox/prototype/exec/java-exec/src/test/resources/qmplans",
      origDirFile.getAbsolutePath()};
    testPlans(planList, outputDirs);
  }


}
