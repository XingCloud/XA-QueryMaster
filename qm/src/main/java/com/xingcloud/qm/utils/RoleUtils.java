package com.xingcloud.qm.utils;

import static com.xingcloud.qm.remote.QueryNode.NODE_CONF_MAP;

import com.xingcloud.qm.remote.QueryNode;
import com.xingcloud.qm.service.QueryWorker;

import java.util.ArrayList;
import java.util.List;

public class RoleUtils {

  public static List<QueryWorker> provideWorkers(String sql) {
    List<QueryNode> clients = NODE_CONF_MAP.get("QUERY-NODES");
    List<QueryWorker> workers = new ArrayList<QueryWorker>(clients.size());
    for (QueryNode client : clients) {
      workers.add(new QueryWorker(sql, client));
    }
    return workers;
  }

}
