package com.xingcloud.qm.service;

import org.apache.drill.common.logical.LogicalPlan;

public interface Submit {
  public boolean submit(String sql, String cacheKey);

  public boolean submit(LogicalPlan plan, String id);
}
