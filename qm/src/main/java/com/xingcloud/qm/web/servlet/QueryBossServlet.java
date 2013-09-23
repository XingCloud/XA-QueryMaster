package com.xingcloud.qm.web.servlet;

import com.caucho.hessian.server.HessianServlet;
import com.xingcloud.qm.exceptions.XRemoteQueryException;
import com.xingcloud.qm.service.QueryMaster;
import com.xingcloud.qm.service.Submit;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class QueryBossServlet extends HessianServlet implements Submit {

  private static final Logger LOGGER = Logger.getLogger(QueryBossServlet.class);

  private static final long serialVersionUID = 5320413547601487799L;

  @Override
  public boolean submit(String cacheKey, String content, SubmitQueryType type) throws XRemoteQueryException {
    switch (type) {
      case SQL:
        LOGGER.info("[WS-SUBMIT] Current type(" + type + ") of operation is not supported.");
        return false;
      case PLAN:
        try {
          LogicalPlan plan = DrillConfig.create().getMapper().readValue(content, LogicalPlan.class);
          if (QueryMaster.getInstance().submit(cacheKey, plan)) {
            LOGGER.info("[WS-SUBMIT] Logical plan is submitted - " + cacheKey);
            return true;
          } else {
            LOGGER.info("[WS-SUBMIT] Logical plan is rejected because it's already in queue - " + cacheKey);
            return false;
          }
        } catch (IOException e) {
          throw new XRemoteQueryException(e);
        }
      default:
        return false;
    }
  }

  @Override
  public boolean submitBatch(Map<String, String> batch, SubmitQueryType type) throws XRemoteQueryException {
    switch (type) {
      case SQL:
        LOGGER.info("[WS-SUBMIT] Current type(" + type + ") of operation is not supported.");
        return false;
      case PLAN:
        Map<String, LogicalPlan> batchPlan = new HashMap<String, LogicalPlan>();
        for (Map.Entry<String, String> entry : batch.entrySet()) {
          String cacheId = entry.getKey();
          String planString = entry.getValue();

          try {
            LogicalPlan plan = DrillConfig.create().getMapper().readValue(planString, LogicalPlan.class);
            batchPlan.put(cacheId, plan);
          } catch (IOException e) {
            throw new XRemoteQueryException(e);
          }
        }
        return QueryMaster.getInstance().submit(batchPlan);
      default:
        return false;
    }
  }
}
