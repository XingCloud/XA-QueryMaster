package com.xingcloud.qm.service;

import com.xingcloud.cache.XCache;
import com.xingcloud.cache.XCacheInfo;
import com.xingcloud.cache.exception.XCacheException;
import com.xingcloud.cache.redis.NoSelectRedisXCacheOperator;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.drill.exec.rpc.user.QueryResultBatch;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 接收前端提交的plan，由QueryMaster控制查询的队列。
 * * 不接受已经提交、且正在查询的的重复query。
 * * 控制集群正在进行的查询计算量。
 * * 控制单个项目正在进行的查询数。
 * * 提交的查询请求进行排序，合并。
 * * 监控查询的完成状况，填到cache里面去。
 */
public class QueryMaster implements Submit, QueryListener {

  //同时最多允许多少个plan执行
  public static final int MAX_PLAN_EXECUTING = 16;

  //每个project，同时最多允许多少个plan执行
  public static final int MAX_PLAN_PER_PROJECT = 1;  
  
  //最多允许多少个plan一起合并
  public static final int MAX_BATCHMERGE = Integer.MAX_VALUE;  
  
  
  private static QueryMaster instance = new QueryMaster();
  
  /**
   * 所有已经提交的任务。
   * 由QueryMaster写入和删除。
   */
  public Map<String, QuerySubmission> submitted = new ConcurrentHashMap<>();


  /**
   * 每个project所提交的任务队列。
   * 由QueryMaster写入，由Scheduler取出。
   */
  public Map<String, Deque<LogicalPlan>> perProjectSubmitted = new ConcurrentHashMap<>();
  
  
  private Scheduler scheduler = new Scheduler();
  
  
  public static QueryMaster getInstance() {
    return instance;
  }

  public QueryMaster() {
    startup();
  }

  private void startup() {
    this.scheduler.start();
  }

  public boolean submitPlainSql(String sql, String cacheKey) {
    return false;  //TODO method implementation
  }

  public boolean submitLogicalPlan(LogicalPlan plan, String id) {
    Object previous = null;
    if(cached(previous)){
      return true;
    }
    if(submitted.containsKey(id)){
      return true;
    }
    enQueue(plan, id);
    return true;
  }

  private void enQueue(LogicalPlan plan, String id) {
    QuerySubmission submission = new BasicQuerySubmission(plan, id);
    submitted.put(id, submission);
    String projectID = PlanMerge.getProjectID(plan);
    putProjectQueue(submission, projectID, id);
  }

  private void putProjectQueue(QuerySubmission submittion, String projectID, String id) {
   getProjectQueue(projectID).add(submittion.plan);
  }
  
  private Deque<LogicalPlan> getProjectQueue(String projectID) {
    Deque<LogicalPlan> projectPlans = perProjectSubmitted.get(projectID);
    if(projectPlans == null){
      projectPlans = new ArrayDeque<>();
      perProjectSubmitted.put(projectID, projectPlans);
    }
    return projectPlans;
  }
  

  private boolean cached(Object previous) {
    return false;  //TODO method implementation
  }
  
  @Override
  public void onQueryResultRecieved(String queryID, QuerySubmission query){
    if(query instanceof BasicQuerySubmission){
      //修改submitted 记录
      BasicQuerySubmission basicQuery = (BasicQuerySubmission) query;
      if(!submitted.containsKey(queryID)){
        throw new IllegalArgumentException("queryID:"+queryID+" not in submitted pool!");
      }
      submitted.remove(queryID);
      String key = queryID;
      Map<String, Number[]> value;
      //TODO put to cache
      try {
        NoSelectRedisXCacheOperator.getInstance().putCache(new XCache(key, basicQuery.value, System.currentTimeMillis(), XCacheInfo.CACHE_INFO_0));
      } catch (XCacheException e) {
        e.printStackTrace();  //e:
      }
    }
    
  }

  public void shutDown(){
    this.scheduler.setStop(true);
  }
  
  class Scheduler extends Thread implements QueryListener{

    /**
     * 正在执行的查询。
     */
    AtomicInteger executing = new AtomicInteger();
    /**
     * 对每个项目，正在执行的查询的计数。
     */
    Map<String, AtomicInteger> perProjectExecuting = new ConcurrentHashMap<>();
    
    private boolean stop = false;
    @Override
    public void run() {
      while (!stop){
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          continue;
        }
        if(executing.intValue()>=MAX_PLAN_EXECUTING //到达最大执行上限
          || submitted.size()==0){ //无任务可以提交
          continue;
        }
        for (Map.Entry<String, Deque<LogicalPlan>> entry : perProjectSubmitted.entrySet()) {
          if(executing.intValue()>=MAX_PLAN_EXECUTING){ //到达最大执行上限
            break;
          }
          String projectID = entry.getKey();
          Deque<LogicalPlan> projectSubmissions = entry.getValue();
          if(projectSubmissions.size()==0             //这个project无任务可提交          
            || perProjectExecuting.get(projectID).intValue() >= MAX_PLAN_PER_PROJECT){//这个任务已经有太多plan在执行
            continue;
          }
          //找任务，合并。不超过MAX_BATCHMERGE
          List<LogicalPlan> pickedPlans = new ArrayList<>();
          for (int i = 0; projectSubmissions.size()>0 && i<MAX_BATCHMERGE; i++) {
            LogicalPlan submittion = projectSubmissions.pollFirst();
            pickedPlans.add(submittion);
          }
          
          Map<LogicalPlan, LogicalPlan> origin2Merged = PlanMerge.sortAndMerge(pickedPlans);
          int executed = 0;
          Iterator<LogicalPlan> merged = origin2Merged.values().iterator();
          for(int i = getProjectCounter(projectID).intValue();i<MAX_PLAN_PER_PROJECT;i++){
            if(!merged.hasNext()){
              break;
            }
            LogicalPlan plan = merged.next();
            doSubmitExecution(projectID, plan);
          }
          //如果有未提交的任务，一并放回perProject的任务队列
          for(;merged.hasNext();){
            LogicalPlan unExecuted = merged.next();
            projectSubmissions.addFirst(unExecuted);
          }
        }
      }
    }

    private void doSubmitExecution(String projectID, LogicalPlan plan) {
      //更新各种counter
      this.executing.incrementAndGet();
      getProjectCounter(projectID).incrementAndGet();
      
      PlanExecutor.getInstance().executePlan(projectID, plan, Scheduler.this);
    }

    private AtomicInteger getProjectCounter(String projectID) {
      AtomicInteger counter = this.perProjectExecuting.get(projectID);
      if(counter == null){
        counter = new AtomicInteger(0);
        perProjectExecuting.put(projectID, counter);
      }
      return counter;
    }

    void setStop(boolean stop) {
      this.stop = stop;
      this.interrupt();
    }

    @Override
    public void onQueryResultRecieved(String queryID, QuerySubmission query) {
      if(query instanceof PlanSubmission){
        PlanSubmission planSubmission = (PlanSubmission) query;
        //修改scheduler计数器
        executing.decrementAndGet();
        getProjectCounter(planSubmission.projectID).decrementAndGet();
        
        // 分发数据
        Map<String, Map<String, Number[]>> materializedRecords = planSubmission.getValues();
        for (Map.Entry<String, Map<String, Number[]>> entry : materializedRecords.entrySet()) {
          String basicQueryID = entry.getKey();
          Map<String, Number[]> value = entry.getValue();
          BasicQuerySubmission basicSubmission = (BasicQuerySubmission) submitted.get(basicQueryID);
          basicSubmission.value = value;
          QueryMaster.this.onQueryResultRecieved(basicQueryID, basicSubmission);
        }
      }
    }
  }
}
