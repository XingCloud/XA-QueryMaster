package com.xingcloud.qm.queue;

import java.util.List;

/**
 * User: Z J Wu Date: 13-7-19 Time: 下午5:49 Package: com.xingcloud.qm.queue
 */
public class QueryTask<T> {

  private List<T> queries;

  private int size;

  public QueryTask(List<T> queries) {
    this.queries = queries;
    this.size = queries.size();
  }

}
