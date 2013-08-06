package com.xingcloud.qm.service;

public interface QueryListener {
  void onQueryResultRecieved(String queryID, QuerySubmission query);
}
