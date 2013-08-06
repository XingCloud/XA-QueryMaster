package com.xingcloud.qm.service;

import org.apache.drill.common.logical.LogicalPlan;

public interface Submit {
  public boolean submitPlainSql(String sql, String cacheKey);

  public boolean submitLogicalPlan(LogicalPlan plan, String id);
}
