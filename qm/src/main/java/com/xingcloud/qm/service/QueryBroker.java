package com.xingcloud.qm.service;

import static com.xingcloud.basic.mail.XMail.sendNewExceptionMail;
import static com.xingcloud.qm.utils.QueryMasterCommonUtils.converet2Cache;
import static com.xingcloud.qm.utils.QueryMasterCommonUtils.hasGroupByKeyWord;
import static com.xingcloud.qm.utils.QueryMasterCommonUtils.parseSqlMeta;
import static com.xingcloud.qm.utils.RoleUtils.provideWorkers;
import static com.xingcloud.qm.utils.UnionUtils.union;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import com.xingcloud.cache.MappedXCache;
import com.xingcloud.qm.exceptions.XRemoteQueryException;
import com.xingcloud.qm.queue.QueryJob;
import com.xingcloud.qm.queue.QueueContainer;
import com.xingcloud.qm.redis.CachePutQueue;
import com.xingcloud.qm.thread.WorkerESProvider;
import org.apache.hadoop.io.MapWritable;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class QueryBroker implements Runnable {

  private static final Logger LOGGER = Logger.getLogger(QueryBroker.class);

  private MapWritable query(String sql, boolean hasGroupBy, int[] metaInfoArray) throws XRemoteQueryException {
    ExecutorService service = WorkerESProvider.getService();
    List<QueryWorker> workers = provideWorkers(sql);
    List<Future<MapWritable>> workList = new ArrayList<Future<MapWritable>>(workers.size());
    Future<MapWritable> undoneWork = null;
    for (QueryWorker worker : workers) {
      undoneWork = service.submit(worker);
      workList.add(undoneWork);
    }

    MapWritable mw = null;

    long before = 0;
    long after = 0;

    long elapsed = 0;
    long wait = 60 * 1000;
    long remain = wait;
    List<MapWritable> rpList = null;

    long t1 = System.currentTimeMillis();
    try {
      for (Future<MapWritable> f : workList) {
        before = System.currentTimeMillis();
        mw = f.get(remain, MILLISECONDS);
        after = System.currentTimeMillis();

        elapsed += after - before;
        remain = wait - elapsed;

        if (rpList == null) {
          rpList = new ArrayList<MapWritable>();
        }
        rpList.add(mw);
      }
    } catch (Exception e) {
      sendNewExceptionMail(e);
      throw new XRemoteQueryException(e);
    }
    long t2 = System.currentTimeMillis();
    MapWritable union = union(rpList, hasGroupBy, metaInfoArray);
    LOGGER.info("[BROKER] - Sql job done in " + (t2 - t1) + " milliseconds - " + sql);
    return union;
  }

  @Override
  public void run() {
    QueueContainer qc = QueueContainer.getInstance();
    QueryJob job = null;
    MapWritable queryResult = null;
    MappedXCache mxc = null;
    int[] metaInfoArray = null;
    String cacheKey = null;
    String sql = null;
    boolean hasGroupBy = false;
    try {
      while (true) {
        job = qc.fetchOne();
        if (job == null) {
          continue;
        }
        cacheKey = job.getCacheKey();
        sql = job.getSql();
        hasGroupBy = hasGroupByKeyWord(sql);
        metaInfoArray = parseSqlMeta(sql, hasGroupBy);
        try {
          queryResult = query(sql, hasGroupBy, metaInfoArray);
        } catch (XRemoteQueryException e) {
          e.printStackTrace();
          continue;
        }
        mxc = converet2Cache(cacheKey, queryResult, metaInfoArray);
        CachePutQueue.getInstance().putQueue(mxc);
      }
    } catch (InterruptedException e) {
      LOGGER.info("[BROKER] - Query broker(" + Thread.currentThread().getName() + ") will be shutdown.");
      Thread.currentThread().interrupt();
    }
  }

}
