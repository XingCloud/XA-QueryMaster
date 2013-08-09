package com.xingcloud.qm.web.servlet;

import com.caucho.hessian.server.HessianServlet;
import com.xingcloud.qm.exceptions.XRemoteQueryException;
import com.xingcloud.qm.service.QueryMaster;
import com.xingcloud.qm.service.Submit;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.log4j.Logger;

import java.io.IOException;

public class QueryBossServlet extends HessianServlet implements Submit {

  private static final Logger LOGGER = Logger.getLogger(QueryBossServlet.class);

  private static final long serialVersionUID = 5320413547601487799L;

  @Override
  public boolean submit(String cacheKey, String content, SubmitQueryType type) throws XRemoteQueryException {
    switch (type) {
      case SQL:
        break;
      case PLAN:
        try {
          LogicalPlan plan = DrillConfig.create().getMapper().readValue(content, LogicalPlan.class);
          QueryMaster.getInstance().submit(cacheKey, plan);
        } catch (IOException e) {
          throw new XRemoteQueryException(e);
        }
    }
    return false;
  }
}
