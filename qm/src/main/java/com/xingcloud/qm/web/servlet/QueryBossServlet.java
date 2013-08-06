package com.xingcloud.qm.web.servlet;

import com.caucho.hessian.server.HessianServlet;
import com.xingcloud.qm.queue.QueryJob;
import com.xingcloud.qm.queue.QueueContainer;
import com.xingcloud.qm.service.QueryMaster;
import com.xingcloud.qm.service.Submit;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.log4j.Logger;

public class QueryBossServlet extends HessianServlet implements Submit {

  private static final Logger LOGGER = Logger.getLogger(QueryBossServlet.class);

  private static final long serialVersionUID = 5320413547601487799L;

  @Override
  public boolean submitPlainSql(String sql, String cacheKey) {
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
  public boolean submitLogicalPlan(LogicalPlan plan, String id) {
    QueryMaster.getInstance().submitLogicalPlan(plan, id);
    return true;
  }

}
