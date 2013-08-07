package com.xingcloud.qm.utils;

import com.xingcloud.qm.remote.QueryNode;
import com.xingcloud.qm.service.QueryWorker;

import java.util.ArrayList;
import java.util.List;

public class RoleUtils {

  public static List<QueryWorker> provideWorkers(String sql) {
    List<QueryWorker> workers = new ArrayList<QueryWorker>(QueryNode.NODES.size());
    for (QueryNode client : QueryNode.NODES) {
      workers.add(new QueryWorker(sql, client));
    }
    return workers;
  }

}
