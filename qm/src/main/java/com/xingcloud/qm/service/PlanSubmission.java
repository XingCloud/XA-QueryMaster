package com.xingcloud.qm.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.xingcloud.qm.result.ResultTable;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * 提交给drill的查询。可能由一个或者多个BasicQuerySubmission合并而来。
 * 
 */
public class PlanSubmission extends QuerySubmission {
  
  String projectID;
  
  //得到的结果。可能包含多个queryID对应的查询结果。
  public Map<String, ResultTable> queryID2Table;

  //记录queryID和对应的原始logical plan
  public Map<String, LogicalPlan> queryIdToPlan = new HashMap<>();
  //是否需要采样
  public boolean needSample = true;
  //是否全部sub plan都满足采样阈值，查询结束
  public boolean allFinish = false;

  /**
   * construct from LogicalPlan, submission id, project id.
   * @param plan
   * @param id
   * @param projectID
   */
  public PlanSubmission(LogicalPlan plan, String id, String projectID) {
    super(plan, id);
    this.projectID = projectID;
  }

  /**
   * construct from LogicalPlan, project id. generate submission id automatically.
   * @param plan
   * @param projectID
   */
  public PlanSubmission(LogicalPlan plan, String projectID){
    this(plan, newIdFromProjectID(projectID), projectID);
  }

  /**
   * construct from another submission object.
   * @param submission
   * @param projectID
   */
  public PlanSubmission(QuerySubmission submission, String projectID) {
    this(submission.plan, projectID);
    this.projectID = projectID;
    absorbIDCost(submission);
  }
  
  private static String newIdFromProjectID(String projectID) {
    return projectID+"."+System.currentTimeMillis();
  }

  public void absorbIDCost(QuerySubmission submission){
    this.cost += submission.cost;
  }

  public void release(){
    this.plan = null;
  }

  public Map<String, ResultTable> getValues() {
    return queryID2Table;
  }

  public void addOriginPlan(String id, LogicalPlan plan) {
    queryIdToPlan.put(id, plan);
  }
  
  
}
