package com.xingcloud.qm.service;

import com.xingcloud.qm.result.ResultTable;
import org.apache.drill.common.logical.LogicalPlan;

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
  
  public Set<String> originalSubmissions = new HashSet<>();

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
    if(submission instanceof PlanSubmission){
      this.originalSubmissions.addAll(((PlanSubmission)submission).originalSubmissions);
    }else{
      this.originalSubmissions.add(submission.id);
    }    
    this.cost += submission.cost;    
  }

  public void release(){
    this.plan = null;
  }

  public Map<String, ResultTable> getValues() {
    return queryID2Table;
  }
  
  
}
