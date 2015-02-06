package com.xingcloud.qm.service;

import static com.xingcloud.qm.remote.QueryNode.LOCAL_DEFAULT_DRILL_CONFIG;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.xingcloud.maincache.MapXCache;
import com.xingcloud.maincache.XCacheException;
import com.xingcloud.maincache.redis.RedisXCacheOperator;
import com.xingcloud.qm.config.QMConfig;
import com.xingcloud.qm.exceptions.XRemoteQueryException;
import com.xingcloud.qm.result.ResultRow;
import com.xingcloud.qm.result.ResultTable;
import com.xingcloud.qm.utils.LogicalPlanUtil;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.log4j.Logger;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 接收前端提交的plan，
 * - 由QueryMaster控制查询的队列。
 * - 不接受已经提交、且正在查询的的重复query。
 * - 控制集群正在进行的查询计算量。
 * - 控制单个项目正在进行的查询数。
 * - 提交的查询请求进行排序，合并。
 * - 监控查询的完成状况，填到cache里面去。
*/
public class QueryMaster implements QueryListener {

  private static final Logger logger = Logger.getLogger(QueryMaster.class);

  //同时最多允许多少个plan执行
//  public static int MAX_PLAN_EXECUTING = 8;
  public static int MAX_PLAN_EXECUTING = 10;

  //每个project，同时最多允许多少个plan执行
//  public static int MAX_PLAN_PER_PROJECT = 1;
  public static int MAX_PLAN_PER_PROJECT = 1;

  //最多允许多少个plan一起合并
//  public static int MAX_BATCHMERGE = Integer.MAX_VALUE;
  public static int MAX_BATCHMERGE = 200;

  //最多允许的合并后的plan的cost。目前，单个原始查询的cost为1。
  public static int MAX_BATCHCOST = 256;

  // 查询结束后是否存放缓存
  public static boolean USING_CACHE;

  private static QueryMaster instance = new QueryMaster();


  /**
   * 所有已经提交的任务。 由QueryMaster写入和删除。
   * cache key --> query submission
   */
  public Map<String, QuerySubmission> submitted = new ConcurrentHashMap<String, QuerySubmission>();

  /**
   * 对每个项目，正在执行的查询的计数。
   * project name --> query count
   */
  public Map<String, AtomicInteger> perProjectExecuting = new ConcurrentHashMap<String, AtomicInteger>();

  /**
   * 正在执行的查询。
   */
  public AtomicInteger executing = new AtomicInteger();

  /**
   * 每个project所提交的任务队列。 由QueryMaster写入，由Scheduler取出。
   * project name --> query submission queue
   */
  public Map<String, Deque<QuerySubmission>> perProjectSubmitted = new ConcurrentHashMap<>();

  //public Map<String,QuerySubmission> executingPlans

  private Scheduler scheduler = new Scheduler("QueryMaster-Scheduler", LOCAL_DEFAULT_DRILL_CONFIG);

  public static QueryMaster getInstance() {
    return instance;
  }

  public QueryMaster() {
    initConfig();
    startup();
  }

    public String getStatus(){
        //正在执行的项目
        //每个项目，正在执行的数量，队列数量
        StringBuilder sb = new StringBuilder("");
        sb.append("executing project: ").append(executing.get()).append("\n");
        sb.append("project info: \n");
        for(Map.Entry<String, Deque<QuerySubmission>> p : perProjectSubmitted.entrySet()){
            sb.append(p.getKey()).append(", queue: ")
                    .append(p.getValue().size()).append(", executing: ")
                    .append(perProjectExecuting.get(p.getKey()) == null ? 0 : perProjectExecuting.get(p.getKey()).get())
                    .append("\n");
        }
        return sb.toString();
    }

    private void initConfig() {
        MAX_PLAN_PER_PROJECT = QMConfig.conf().getInt(QMConfig.MAX_PLAN_PER_PROJECT, MAX_PLAN_PER_PROJECT);
        logger.info("init " + QMConfig.MAX_PLAN_PER_PROJECT + " " + MAX_PLAN_PER_PROJECT);
        MAX_PLAN_EXECUTING = QMConfig.conf().getInt(QMConfig.MAX_PLAN_EXECUTING, MAX_PLAN_EXECUTING);
        logger.info("init " + QMConfig.MAX_PLAN_EXECUTING + " " + MAX_PLAN_EXECUTING);
        MAX_BATCHCOST = QMConfig.conf().getInt(QMConfig.MAX_BATCHCOST, MAX_BATCHCOST);
        logger.info("init " + QMConfig.MAX_BATCHCOST + " " + MAX_BATCHCOST);
        MAX_BATCHMERGE = QMConfig.conf().getInt(QMConfig.MAX_BATCHMERGE, MAX_BATCHMERGE);
        logger.info("init " + QMConfig.MAX_BATCHMERGE + " " + MAX_BATCHMERGE);
        USING_CACHE = QMConfig.conf().getBoolean(QMConfig.USING_CACHE, USING_CACHE);
        logger.info("init " + QMConfig.USING_CACHE + " " + USING_CACHE);
    }

  private void startup() {
    this.scheduler.start();
  }

  public synchronized boolean submit(String cacheKey, LogicalPlan logicalPlan) throws XRemoteQueryException {
    if (!submitted.containsKey(cacheKey)) {
      logger.info("Add " + cacheKey + " to queue.");
      enQueue(logicalPlan, cacheKey);
      return true;
    }
    return false;
  }

  public synchronized boolean submit(Map<String, LogicalPlan> batch) {
    List<QuerySubmission> submissions = new ArrayList<>();
    String pID = null;
    for (Map.Entry<String, LogicalPlan> entry : batch.entrySet()) {
      String cacheKey = entry.getKey();
      if (!submitted.containsKey(cacheKey)) {
        logger.info("Add " + cacheKey + " to queue.");
        LogicalPlan plan = entry.getValue();
        if (pID == null) {
          pID = PlanMerge.getProjectID(plan);
        }
        QuerySubmission submission = new BasicQuerySubmission(plan, cacheKey);
        submitted.put(cacheKey, submission);
        submissions.add(submission);
      } else {
        logger.info("Reject " + cacheKey + " because it is already in queue.");
      }



    }
    if (submissions.size() > 0) {
      putProjectQueue(submissions, pID);
    }
    return submissions.size() > 0;
  }

  public Set<LogicalPlan> getQueuePlans() {
    Set<LogicalPlan> rets = new HashSet<>();
    for (Map.Entry<String, QuerySubmission> entry : submitted.entrySet()) {
      rets.add(entry.getValue().plan);
    }
    return rets;
  }

  public Set<LogicalPlan> getExecutingPlans() {
    return null;
  }

  private void enQueue(LogicalPlan plan, String id) {
    QuerySubmission submission = new BasicQuerySubmission(plan, id);
    submitted.put(id, submission);
    String projectID = PlanMerge.getProjectID(plan);
    putProjectQueue(submission, projectID, id);
  }

  private void putProjectQueue(QuerySubmission submission, String projectID, String id) {
    Deque<QuerySubmission> projectQueue = getProjectQueue(projectID);
    synchronized (projectQueue) {
      projectQueue.add(submission);
    }
  }

  private void putProjectQueue(List<QuerySubmission> submissions, String projectID) {
    logger.info("Submit " + submissions.size() + " to queue of " + projectID);
    Deque<QuerySubmission> projectQueue = getProjectQueue(projectID);
    synchronized (projectQueue) {
      projectQueue.addAll(submissions);
    }
  }

  private synchronized Deque<QuerySubmission> getProjectQueue(String projectID) {
    Deque<QuerySubmission> projectPlans = perProjectSubmitted.get(projectID);
    if (projectPlans == null) {
      //todo: ArrayDeque does not automatically shrink?
      projectPlans = new ArrayDeque<>();
      perProjectSubmitted.put(projectID, projectPlans);
    }
    return projectPlans;
  }

  @Override
  public void onQueryResultReceived(String queryID, QuerySubmission query) {
    if (query instanceof BasicQuerySubmission) {
      //修改submitted 记录
      logger.info("BasicQuerySubmission " + queryID + " completed.");
      BasicQuerySubmission basicQuery = (BasicQuerySubmission) query;
      if (!submitted.containsKey(queryID)) {
        throw new IllegalArgumentException("queryID:" + queryID + " not in submitted pool!");
      }
      String key = queryID;
      if (basicQuery.e != null) {
        logger.warn("execution failed!", basicQuery.e);
        if (USING_CACHE) {
          try {
            RedisXCacheOperator.getInstance().putExceptionCache(queryID);
            logger.info("[X-CACHE] - Exception placeholder of " + key + " has been added to main cache.");
          } catch (XCacheException e) {
            e.printStackTrace();
          }
        }
      } else {
        logger.info("basicQuery Result");
        MapXCache xCache = null;
        if (((BasicQuerySubmission) query).value.isEmpty()) {
          logger.info("[X-CACHE] - Result of " + key + " is empty.");
          try {
            xCache = MapXCache.buildMapXCache(key, null);
          } catch (XCacheException e) {
            e.printStackTrace();
          }
        } else {
          for (Map.Entry<String, ResultRow> entry : ((BasicQuerySubmission) query).value.entrySet()) {
            //String queryId=entry.getKey();
            ResultRow result = entry.getValue();

            logger.info("[RESULT-INFO] - " + queryID + " - key - " + entry
              .getKey() + " - ResultTuple[" + result.count + "#" + result.sum + "#" +
                          result.userNum + "@" + result.sampleRate + "]");
          }
          try {
            xCache = MapXCache.buildMapXCache(key, basicQuery.value.toCacheValue());
          } catch (XCacheException e) {
            e.printStackTrace();
          }
        }
        if (USING_CACHE) {
          try {
            RedisXCacheOperator.getInstance().putMapCache(xCache);
            logger.info("[X-CACHE] - Result of " + key + "has been added to main cache.");
          } catch (XCacheException e) {
            e.printStackTrace();
          }
        }
      }
      submitted.remove(queryID);
    }
  }

  public void shutDown() {
    this.scheduler.setStop(true);
  }

  class Scheduler extends Thread implements QueryListener {

    private volatile boolean stop = false;

    private DrillConfig config;

    ThreadPoolExecutor mergeAndSubmitExecutor;

    Scheduler(String name, DrillConfig config) {
      super(name);
      this.config = config;
      ThreadFactoryBuilder builder = new ThreadFactoryBuilder();

      builder.setNameFormat("Merge and submit pool");
      builder.setDaemon(true);
      ThreadFactory factory = builder.build();
      mergeAndSubmitExecutor = (ThreadPoolExecutor)Executors.newFixedThreadPool(MAX_PLAN_EXECUTING, factory);
    }

    @Override
    public void run() {
      logger.info("QueryMaster scheduler starting...");
      while (!stop) {
        if (executing.intValue() >= MAX_PLAN_EXECUTING || submitted.size() == 0) {
          //正在执行的plan个数达到最大值 或者 没有查询任务
          try {
            Thread.sleep(100);
          } catch (InterruptedException iex) {
            continue;
          }
        }
        for (Map.Entry<String, Deque<QuerySubmission>> entry : perProjectSubmitted.entrySet()) {
          //to control total executing plans
          if (executing.intValue() >= MAX_PLAN_EXECUTING) { //到达最大执行上限
            break;
          }
          String projectID = entry.getKey();
          Deque<QuerySubmission> projectSubmissions = entry.getValue();
          if (projectSubmissions.size() == 0             //这个project无任务可提交
            || getProjectCounter(projectID).intValue() >= MAX_PLAN_PER_PROJECT) {//这个任务已经有太多plan在执行
            continue;
          }

          incCounter(projectID);
          Thread mergeAndSubmitTask = new MergeAndSubmit(projectSubmissions, projectID, this);
          mergeAndSubmitExecutor.execute(mergeAndSubmitTask);
        }
      }
      logger.info("QueryMaster scheduler exiting...");
    }

    //todo: this method should belong to MergeAndSubmit
    private void doSubmitExecution(PlanSubmission plan) {
      PlanExecutor.getInstance().executePlan(plan, Scheduler.this);
    }

    private void incCounter(String projectID) {
      //更新各种counter
      executing.incrementAndGet();
      getProjectCounter(projectID).incrementAndGet();
    }

    private AtomicInteger getProjectCounter(String projectID) {
      AtomicInteger counter = perProjectExecuting.get(projectID);
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
        PlanSubmission planSubmission = (PlanSubmission) query;
        //所有sub plan查询结束，修改scheduler计数器
        if (((PlanSubmission) query).allFinish) {
          executing.decrementAndGet();
          getProjectCounter(planSubmission.projectID).decrementAndGet();
        }

        // 分发数据
        if (planSubmission.e != null || planSubmission.getValues() == null || (planSubmission.getValues().size() == 0 && planSubmission.allFinish)) {
          logger.info("PlanSubmission " + queryID + " completed.");
          //出错处理
          for (String basicQueryID : planSubmission.queryIdToPlan.keySet()) {
            if (planSubmission.finishedIDSet.contains(basicQueryID)) {
              continue;
            }
            //未完成或查询到的请求
            BasicQuerySubmission basicSubmission = (BasicQuerySubmission) submitted.get(basicQueryID);
            basicSubmission.e = planSubmission.e;
            //Drill-bit 返回empty set
            if (basicSubmission.e == null) {
              basicSubmission.value = new ResultTable();
              basicSubmission.value.put("XA-NA", new ResultRow(0, 0, 0));
              logger.info("PlanSubmission " + basicQueryID + " completed with empty result.");
            }
            QueryMaster.this.onQueryResultReceived(basicQueryID, basicSubmission);
          }
        } else {
          Map<String, ResultTable> materializedRecords = planSubmission.getValues();
          for (String basicQueryID : planSubmission.queryID2Table.keySet()) {
            ResultTable value = materializedRecords.get(basicQueryID);
            BasicQuerySubmission basicSubmission = (BasicQuerySubmission) submitted.get(basicQueryID);
            if(basicSubmission==null){
              logger.info("basicQueryId--"+basicQueryID+" does not exists in submitted");
              continue;
            }
            basicSubmission.value = value;

            if (value == null) {
              //Drill-bit 返回empty set
              logger.info("PlanSubmission " + basicQueryID + "completed with empty result.");
              basicSubmission.value = new ResultTable();
              basicSubmission.value.put("XA-NA", new ResultRow(0, 0, 0));
            }
            QueryMaster.this.onQueryResultReceived(basicQueryID, basicSubmission);
          }
          if (planSubmission.allFinish) {
            //最后一轮需要把drill-bit没有返回的结果补0
            Set<String> allQueryIds = planSubmission.queryIdToPlan.keySet();
            allQueryIds.removeAll(planSubmission.finishedIDSet);
            for (String basicQueryID : allQueryIds) {
              BasicQuerySubmission basicSubmission = (BasicQuerySubmission) submitted.get(basicQueryID);
              basicSubmission.value = new ResultTable();
              basicSubmission.value.put("XA-NA", new ResultRow(0, 0, 0));
              logger.info("PlanSubmission " + basicQueryID + " completed with empty result.");
              QueryMaster.this.onQueryResultReceived(basicQueryID, basicSubmission);
            }
          }

        }
      }
    }
  }

  public void clearSubmittedTag(PlanSubmission planSubmission) {
    for (String basicQueryID : planSubmission.queryIdToPlan.keySet()) {
      logger.warn("------ Force to remove basic query id: " + basicQueryID);
      submitted.remove(basicQueryID);
    }
    //更新counter
    perProjectExecuting.get(planSubmission.projectID).decrementAndGet();
    executing.decrementAndGet();
  }

  class MergeAndSubmit extends Thread {
    private Deque<QuerySubmission> projectSubmissions;
    private String pID;
    private Scheduler scheduler;

    public MergeAndSubmit(Deque<QuerySubmission> projectSubmissions, String pID, Scheduler scheduler) {
      this.projectSubmissions = projectSubmissions;
      this.pID = pID;
      this.scheduler = scheduler;
    }

    @Override
    public void run() {
      synchronized (projectSubmissions) {
        logger.info("Start to process " + pID + "\trequest size: " + projectSubmissions.size());

        //找任务，合并。不超过MAX_BATCHMERGE，MAX_BATCHCOST
        List<QuerySubmission> pickedSubmissions = new ArrayList<>();
        List<LogicalPlan> pickedPlans = new ArrayList<>();
        int totalCost = 0;

        //cache key --> logical plan
        Map<String, LogicalPlan> id2Origin = new HashMap<>();
        for (int i = 0; projectSubmissions.size() > 0 && i < MAX_BATCHMERGE && totalCost < MAX_BATCHCOST; i++) {
          QuerySubmission submission = projectSubmissions.pollFirst();
          totalCost += submission.cost;
          pickedSubmissions.add(submission);
          pickedPlans.add(submission.plan);
          if (!(submission instanceof PlanSubmission)) {
            id2Origin.put(submission.id, LogicalPlanUtil.copyPlan(submission.plan));
          }
        }

        try {
          // original plan --> merged plan
          Map<LogicalPlan, LogicalPlan> origin2Merged = PlanMerge.sortAndMerge(pickedPlans, scheduler.config);

          //建立合并后的plan和原始用户提交的BasicQuerySubmission之间的对应关系
          // merged plan --> plan submission ?
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
                PlanSubmission planSubmission = new PlanSubmission(submission, pID);
                planSubmission.addOriginPlan(submission.id, id2Origin.get(submission.id));
                mergedPlan2Submissions.put(to, planSubmission);
              }
            } else {//newly merged plan
              PlanSubmission mergedSubmission = mergedPlan2Submissions.get(to);
              if (mergedSubmission == null) {
                mergedSubmission = new PlanSubmission(to, pID);
                mergedPlan2Submissions.put(to, mergedSubmission);
              }
              //mark submission merge
              mergedSubmission.absorbIDCost(submission);
              if (submission instanceof PlanSubmission) {
                logger.warn("Plan has already been merged... " + ((PlanSubmission) submission).projectID + "\t" + submission.id);
                //之前已经merge过的plan
                Map<String, LogicalPlan> id2PlanMap = ((PlanSubmission) submission).queryIdToPlan;
                for (Map.Entry<String, LogicalPlan> id2PlanEntry : id2PlanMap.entrySet()) {
                  mergedSubmission.addOriginPlan(id2PlanEntry.getKey(), id2PlanEntry.getValue());
                }
              } else {
                mergedSubmission.addOriginPlan(submission.id, id2Origin.get(submission.id));
              }
            }
          }

          for (PlanSubmission plan : mergedPlan2Submissions.values()) {
            scheduler.doSubmitExecution(plan);
          }
        } catch (Throwable e) {
          e.printStackTrace();
          int size = 0;
          //clear querySubmission from submitted.
          for (QuerySubmission submission : pickedSubmissions) {
            if (submission instanceof PlanSubmission) {
              for (String qID : ((PlanSubmission) submission).queryIdToPlan.keySet()) {
                submitted.remove(qID);
                size++;
              }
            } else {
              submitted.remove(submission.id);
              size++;
            }
          }
          // executing and executing for the pid both decrement.
          // because before mergeAndSubmit the executing and executing for the pid has increase by one.
          executing.decrementAndGet();
          perProjectExecuting.get(pID).decrementAndGet();
          logger.error("Submit logical plan failure! Picked submission size: " + size);
        }
      }
    }
  }
}