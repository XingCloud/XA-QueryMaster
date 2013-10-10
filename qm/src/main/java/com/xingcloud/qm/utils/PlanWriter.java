package com.xingcloud.qm.utils;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Set;

/**
 * Created with IntelliJ IDEA.
 * User: Wang Yufei
 * Date: 13-9-27
 * Time: 下午1:46
 * To change this template use File | Settings | File Templates.
 */
public class PlanWriter {
  private static Logger logger = LoggerFactory.getLogger(PlanWriter.class);

  private static final String basePath = "/data/log/plans/";
  private final SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd-hhmmss");
  private static final String target = "target.json";
  private static final String source = "source.json";

  private String ymd;
  private String hms;

  private String planDir;
  private String targetFilePath;
  private String sourceFilePath;
  private DrillConfig config;

  public PlanWriter(long ts, DrillConfig config) {
    String formatTS = format.format(new Date(ts));
    String[] split = formatTS.split("-");
    ymd = split[0];
    hms = split[1];
    this.planDir = basePath + ymd + "/" + hms + "/";
    File dir = new File(planDir);
    if (!dir.exists()) {
      dir.mkdir();
    }
    targetFilePath = planDir + target;
    sourceFilePath = planDir + source;
    this.config = config;
  }

  public void writeSourcePlans(List<LogicalPlan> plans) throws IOException {
    long st = System.nanoTime();
    File sourceFile = new File(sourceFilePath);
    Writer sourceWriter = null;
    try {
      sourceWriter = new FileWriter(sourceFile);
    for (LogicalPlan plan : plans) {
      sourceWriter.write(config.getMapper().writeValueAsString(plan));
      sourceWriter.write("\n");
    }
    } finally {
      if (sourceWriter != null) {
        sourceWriter.flush();
        sourceWriter.close();
      }
    }
    logger.info("Write source plans taken " + (System.nanoTime()-st)/1.0e9 + " sec");
  }

  public void writeMergedPlan(Collection<LogicalPlan> plans) throws IOException {
    long st = System.nanoTime();
    File targetFile = new File(targetFilePath);
    Writer targetWriter = null;
    try {
      targetWriter = new FileWriter(targetFile);
      for (LogicalPlan plan : plans) {
        targetWriter.write(config.getMapper().writeValueAsString(plan));
        targetWriter.write("\n");
      }
    } finally {
      if (targetWriter != null) {
        targetWriter.flush();
        targetWriter.close();
      }
    }
    logger.info("Write merged plan taken " + (System.nanoTime()-st)/1.0e9 + " sec");
  }

}
