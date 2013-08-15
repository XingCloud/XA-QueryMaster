package com.xingcloud.qm.service;

import com.xingcloud.cache.XCache;
import com.xingcloud.cache.XCacheInfo;
import com.xingcloud.cache.exception.XCacheException;
import com.xingcloud.cache.redis.NoSelectRedisXCacheOperator;
import com.xingcloud.qm.exceptions.XRemoteQueryException;
import com.xingcloud.qm.result.ResultTable;
import org.apache.drill.common.logical.LogicalPlan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 接收前端提交的plan，由QueryMaster控制查询的队列。 
 * - 不接受已经提交、且正在查询的的重复query。 
 * - 控制集群正在进行的查询计算量。 
 * - 控制单个项目正在进行的查询数。 
 * - 提交的查询请求进行排序，合并。
 * - 监控查询的完成状况，填到cache里面去。
 */
public class QueryMaster implements QueryListener {

  static Logger logger = LoggerFactory.getLogger(QueryMaster.class);

  //同时最多允许多少个plan执行
  public static final int MAX_PLAN_EXECUTING = 16;

  //每个project，同时最多允许多少个plan执行
  public static final int MAX_PLAN_PER_PROJECT = 1;

  //最多允许多少个plan一起合并
  public static final int MAX_BATCHMERGE = Integer.MAX_VALUE;

  //最多允许的合并后的plan的cost。目前，单个原始查询的cost为1。
  public static final int MAX_BATCHCOST = 256;

  private static QueryMaster instance = new QueryMaster();

  /**
   * 所有已经提交的任务。 由QueryMaster写入和删除。
   */
  public Map<String, QuerySubmission> submitted = new ConcurrentHashMap<String, QuerySubmission>();

  /**
   * 每个project所提交的任务队列。 由QueryMaster写入，由Scheduler取出。
   */
  public Map<String, Deque<QuerySubmission>> perProjectSubmitted = new ConcurrentHashMap<>();

  private Scheduler scheduler = new Scheduler("QueryMaster-Scheduler");

  public static QueryMaster getInstance() {
    return instance;
  }

  public QueryMaster() {
    startup();
  }

  private void startup() {
    this.scheduler.start();
  }


  public boolean submit(String cacheKey, LogicalPlan logicalPlan) throws XRemoteQueryException {
    if (submitted.containsKey(cacheKey)) {
      return false;
    }
    enQueue(logicalPlan, cacheKey);
    return true;
  }

  private void enQueue(LogicalPlan plan, String id) {
    logger.info("BasicQuerySubmission {} submitted.", id);
    QuerySubmission submission = new BasicQuerySubmission(plan, id);
    submitted.put(id, submission);
    String projectID = PlanMerge.getProjectID(plan);
    putProjectQueue(submission, projectID, id);
  }

  private void putProjectQueue(QuerySubmission submittion, String projectID, String id) {
    getProjectQueue(projectID).add(submittion);
  }

  private Deque<QuerySubmission> getProjectQueue(String projectID) {
    Deque<QuerySubmission> projectPlans = perProjectSubmitted.get(projectID);
    if (projectPlans == null) {
      projectPlans = new ArrayDeque<>();
      perProjectSubmitted.put(projectID, projectPlans);
    }
    return projectPlans;
  }

  @Override
  public void onQueryResultReceived(String queryID, QuerySubmission query) {
    if (query instanceof BasicQuerySubmission) {
      //修改submitted 记录
      logger.info("BasicQuerySubmission {} completed.", queryID);      
      BasicQuerySubmission basicQuery = (BasicQuerySubmission) query;
      if (!submitted.containsKey(queryID)) {
        throw new IllegalArgumentException("queryID:" + queryID + " not in submitted pool!");
      }
      submitted.remove(queryID);
      String key = queryID;
      if (basicQuery.e != null) {
        logger.warn("execution failed!", basicQuery.e);

      } else {
          /*
        try {
          NoSelectRedisXCacheOperator.getInstance().putCache(
           new XCache(key, basicQuery.value.toCacheValue(), System.currentTimeMillis(), XCacheInfo.CACHE_INFO_0));
        } catch (XCacheException e) {
          e.printStackTrace();  //e:
        }*/
      }
    }

  }

  public void shutDown() {
    this.scheduler.setStop(true);
  }

  class Scheduler extends Thread implements QueryListener {

    /**
     * 正在执行的查询。
     */
    AtomicInteger executing = new AtomicInteger();
    /**
     * 对每个项目，正在执行的查询的计数。
     */
    Map<String, AtomicInteger> perProjectExecuting = new ConcurrentHashMap<String, AtomicInteger>();

    private boolean stop = false;

    Scheduler(String name) {
      super(name);
    }

    @Override
    public void run() {
      logger.info("QueryMaster scheduler starting...");
      while (!stop) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          continue;
        }
        if (executing.intValue() >= MAX_PLAN_EXECUTING //到达最大执行上限
          || submitted.size() == 0) { //无任务可以提交
          continue;
        }
        for (Map.Entry<String, Deque<QuerySubmission>> entry : perProjectSubmitted.entrySet()) {
          if (executing.intValue() >= MAX_PLAN_EXECUTING) { //到达最大执行上限
            break;
          }
          String projectID = entry.getKey();
          Deque<QuerySubmission> projectSubmissions = entry.getValue();
          if (projectSubmissions.size() == 0             //这个project无任务可提交
            || getProjectCounter(projectID).intValue() >= MAX_PLAN_PER_PROJECT) {//这个任务已经有太多plan在执行
            continue;
          }
          //找任务，合并。不超过MAX_BATCHMERGE，MAX_BATCHCOST
          List<QuerySubmission> pickedSubmissions = new ArrayList<>();
          int totalCost = 0;
          for (int i = 0; projectSubmissions.size() > 0 && i < MAX_BATCHMERGE && totalCost < MAX_BATCHCOST; i++) {
            QuerySubmission submission = projectSubmissions.pollFirst();
            totalCost += submission.cost;
            pickedSubmissions.add(submission);
          }
          List<LogicalPlan> pickedPlans = new ArrayList<>();
          for (int i = 0; i < pickedSubmissions.size(); i++) {
            QuerySubmission querySubmission = pickedSubmissions.get(i);
            pickedPlans.add(querySubmission.plan);
          }
          Map<LogicalPlan, LogicalPlan> origin2Merged = PlanMerge.sortAndMerge(pickedPlans);
          int executed = 0;

          //建立合并后的plan和原始用户提交的BasicQuerySubmission之间的对应关系
          Map<LogicalPlan, PlanSubmission> mergedPlan2Submissions = new HashMap<>();
          for (int i = 0; i < pickedSubmissions.size(); i++) {
            QuerySubmission submission = pickedSubmissions.get(i);
            LogicalPlan to = origin2Merged.get(submission.plan);
            if (to == submission.plan) {//origin = merged, 即没和别的plan合并的plan
              if (submission instanceof PlanSubmission) {
                //以前已经合并过的plan
                mergedPlan2Submissions.put(to, (PlanSubmission) submission);
              } else {
                //第一次被合并的plan，会变成PlanSubmission
                mergedPlan2Submissions.put(to, new PlanSubmission(submission, projectID));
              }
            } else {//newly merged plan
              PlanSubmission mergedSubmission = mergedPlan2Submissions.get(to);
              if (mergedSubmission == null) {
                mergedSubmission = new PlanSubmission(to, projectID);
                mergedPlan2Submissions.put(to, mergedSubmission);
              }
              //mark submission merge              
              (mergedSubmission).absorbIDCost(submission);              
            }
          }

          Iterator<PlanSubmission> mergedSubmissions = mergedPlan2Submissions.values().iterator();
          for (int i = getProjectCounter(projectID).intValue(); i < MAX_PLAN_PER_PROJECT; i++) {
            if (!mergedSubmissions.hasNext()) {
              break;
            }
            PlanSubmission plan = mergedSubmissions.next();
            if(logger.isDebugEnabled()){
              logger.debug("PlanSubmission {} submitted: {}", plan.id, plan.originalSubmissions.toString());
            }
            doSubmitExecution(plan);
          }
          //如果有未提交的任务，一并放回perProject的任务队列
          for (; mergedSubmissions.hasNext(); ) {
            QuerySubmission unExecuted = mergedSubmissions.next();
            projectSubmissions.addFirst(unExecuted);
          }
        }
      }
      logger.info("QueryMaster scheduler exiting...");      
    }

    private void doSubmitExecution(PlanSubmission plan) {
      //更新各种counter
      this.executing.incrementAndGet();
      getProjectCounter(plan.projectID).incrementAndGet();

      PlanExecutor.getInstance().executePlan(plan, Scheduler.this);
    }

    private AtomicInteger getProjectCounter(String projectID) {
      AtomicInteger counter = this.perProjectExecuting.get(projectID);
      if (counter == null) {
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
    public void onQueryResultReceived(String queryID, QuerySubmission query) {
      if (query instanceof PlanSubmission) {
        logger.debug("PlanSubmission: {} completed.", queryID);
        PlanSubmission planSubmission = (PlanSubmission) query;
        //修改scheduler计数器
        executing.decrementAndGet();
        getProjectCounter(planSubmission.projectID).decrementAndGet();

        // 分发数据
        if (planSubmission.e != null||planSubmission.getValues() == null || planSubmission.getValues().size()==0) {
          //出错处理
          for (String basicQueryID : planSubmission.originalSubmissions) {
            BasicQuerySubmission basicSubmission = (BasicQuerySubmission) submitted.get(basicQueryID);
            basicSubmission.e = planSubmission.e;
            if(basicSubmission.e == null){
              basicSubmission.e = new NullPointerException("haven't received any results for "+basicQueryID+"!");
            }
            QueryMaster.this.onQueryResultReceived(basicQueryID, basicSubmission);
          }
        } else {
          Map<String, ResultTable> materializedRecords = planSubmission.getValues();
          for (String basicQueryID : planSubmission.originalSubmissions) {
            ResultTable value = materializedRecords.get(basicQueryID);
            BasicQuerySubmission basicSubmission = (BasicQuerySubmission) submitted.get(basicQueryID);
            basicSubmission.value = value;
            if(value == null){
              basicSubmission.e = new NullPointerException("haven't received any results for "+basicQueryID+"!");
            }
            QueryMaster.this.onQueryResultReceived(basicQueryID, basicSubmission);
          }
        }
      }
    }
  }
}
