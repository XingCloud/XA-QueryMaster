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
  
  
  public Map<String, ResultTable> queryID2Table;
  
  public Set<String> originalSubmissions = new HashSet<>();
  
  public PlanSubmission(LogicalPlan plan, String id, String projectID) {
    super(plan, id);
    this.projectID = projectID;
  }
  
  public PlanSubmission(LogicalPlan plan, String projectID){
    super(plan, newIdFromProjectID(projectID));
  }

  private static String newIdFromProjectID(String projectID) {
    return projectID+"."+System.currentTimeMillis();
  }

  public PlanSubmission(QuerySubmission submission, String projectID) {
    this(submission.plan, projectID);
    this.projectID = projectID;
    absorbIDCost(submission);
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
