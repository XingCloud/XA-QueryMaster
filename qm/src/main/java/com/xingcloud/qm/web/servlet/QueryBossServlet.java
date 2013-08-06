package com.xingcloud.qm.web.servlet;

import com.caucho.hessian.server.HessianServlet;
import com.xingcloud.qm.queue.QueryJob;
import com.xingcloud.qm.queue.QueueContainer;
import com.xingcloud.qm.service.Submit;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.log4j.Logger;

public class QueryBossServlet extends HessianServlet implements Submit {

  private static final Logger LOGGER = Logger.getLogger(QueryBossServlet.class);

  private static final long serialVersionUID = 5320413547601487799L;

  @Override
  public boolean submit(String sql, String cacheKey) {
    long timestamp = System.currentTimeMillis();
    QueueContainer qc = QueueContainer.getInstance();
    try {
      if (qc.submit(new QueryJob(cacheKey, sql, timestamp))) {
        LOGGER.info("[BOSS] - Sql received and submitted - " + cacheKey + "(" + sql + ")");
      } else {
        LOGGER.info("[BOSS] - Sql received but not submit, in queue already - " + cacheKey + "(" + sql + ")");
      }
      return true;
    } catch (InterruptedException e) {
      e.printStackTrace();
      return false;
    }
  }

  @Override
  public boolean submit(LogicalPlan plan, String id) {
    long timestamp = System.currentTimeMillis();
    QueueContainer qc = QueueContainer.getInstance();
    try {
      if (qc.submit(new QueryJob(plan, timestamp, id))) {
        LOGGER.info("[BOSS] - Logical plan received and submitted - " + id);
      } else {
        LOGGER.info("[BOSS] - Logical plan received but not submit, in queue already - " + id);
      }
      return true;
    } catch (InterruptedException e) {
      e.printStackTrace();
      return false;
    }
  }

}
