package com.xingcloud.qm.queue;

import static com.xingcloud.basic.Constants.DATE_FORMAT_FULL_LONG;
import static org.apache.commons.lang3.time.DateFormatUtils.format;

import org.apache.drill.common.logical.LogicalPlan;

public class QueryJob {

  private LogicalPlan logicalPlan;

  private String cacheKey;

  private String sql;

  private long timestamp;

  public QueryJob(String sql, long timestamp) {
    super();
    this.sql = sql;
    this.timestamp = timestamp;
  }

  public QueryJob(String cacheKey, String sql, long timestamp) {
    super();
    this.cacheKey = cacheKey;
    this.sql = sql;
    this.timestamp = timestamp;
  }

  public QueryJob(LogicalPlan logicalPlan, long timestamp, String cacheKey) {
    this.logicalPlan = logicalPlan;
    this.timestamp = timestamp;
    this.cacheKey = cacheKey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    QueryJob queryJob = (QueryJob) o;

    if (sql != null ? !sql.equals(queryJob.sql) : queryJob.sql != null) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return sql != null ? sql.hashCode() : 0;
  }

  public String getSql() {
    return sql;
  }

  public void setSql(String sql) {
    this.sql = sql;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public void setTimestamp(long timestamp) {
    this.timestamp = timestamp;
  }

  public String getCacheKey() {
    return cacheKey;
  }

  public void setCacheKey(String cacheKey) {
    this.cacheKey = cacheKey;
  }

  public LogicalPlan getLogicalPlan() {
    return logicalPlan;
  }

  @Override
  public String toString() {
    return "QJ(" + format(timestamp, DATE_FORMAT_FULL_LONG) + ")." + sql;
  }

}
