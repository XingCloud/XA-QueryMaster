package com.xingcloud.qm.remote;

import com.xingcloud.basic.conf.ConfigReader;
import com.xingcloud.basic.conf.Dom;
import com.xingcloud.qm.service.QueryWorker;
import org.apache.commons.collections.CollectionUtils;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.DirectBufferAllocator;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;

public class QueryNode {
  private static final Logger LOGGER = Logger.getLogger(QueryWorker.class);
  public static final List<QueryNode> NODES = new ArrayList<>(16);
  public static final DrillConfig LOCAL_DEFAULT_DRILL_CONFIG = DrillConfig.create();
  public static final BufferAllocator DEFAULT_BUFFER_ALLOCATOR = new DirectBufferAllocator();

  static {
    Dom root = null;
    try {
      root = ConfigReader.getDom("nodes.xml");
    } catch (Exception e) {
      e.printStackTrace();
    }
    List<Dom> nodesDomList = root.elements("nodes");

    String id, conf;
    List<Dom> nodes;
    for (Dom nodesDom : nodesDomList) {
      nodes = nodesDom.elements("node");
      for (Dom nodeDom : nodes) {
        id = nodeDom.getAttributeValue("id");
        conf = nodeDom.getAttributeValue("conf");
        NODES.add(new QueryNode(id, conf));
      }
    }
  }

  private String id;

  private DrillClient drillClient;

  public DrillClient getDrillClient() {
    return drillClient;
  }

  public static DrillClient[] getClients() {
    if (CollectionUtils.isEmpty(NODES)) {
      return null;
    }
    DrillClient[] clients = new DrillClient[NODES.size()];
    for (int i = 0; i < NODES.size(); i++) {
      clients[i] = NODES.get(i).getDrillClient();
    }
    return clients;
  }

  public QueryNode(String id, String conf) {
    super();
    this.id = id;
    this.drillClient = new DrillClient(DrillConfig.create(conf));
    try {
      this.drillClient.connect();
    } catch (Exception e) {
      e.printStackTrace();
    }
    LOGGER.info("[DRILL-CLIENT]: " + id + " connected to server.");
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }

  public static BufferAllocator getAllocator() {
    return DEFAULT_BUFFER_ALLOCATOR;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof QueryNode)) {
      return false;
    }

    QueryNode queryNode = (QueryNode) o;

    if (!id.equals(queryNode.id)) {
      return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return id.hashCode();
  }

  @Override
  public String toString() {
    return "QueryClient." + id;
  }

}
