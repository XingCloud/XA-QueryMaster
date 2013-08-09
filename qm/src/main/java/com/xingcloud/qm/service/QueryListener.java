package com.xingcloud.qm.service;

public interface QueryListener {
  void onQueryResultReceived(String queryID, QuerySubmission query);
}
