package com.xingcloud.qm.remote;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.log4j.Logger;

import com.xingcloud.basic.conf.ConfigReader;
import com.xingcloud.basic.conf.Dom;
import com.xingcloud.qm.service.QuerySlaveProtocol;
import com.xingcloud.qm.service.QueryWorker;

public class QueryNode {
    private static final Logger LOGGER = Logger.getLogger(QueryWorker.class);
    public static final Map<String, List<QueryNode>> NODE_CONF_MAP = new HashMap<String, List<QueryNode>>();
    static {
        Dom root = ConfigReader.getDom("nodes.xml");
        List<Dom> nodesDomList = root.elements("nodes");

        String gid = null;
        String id = null;
        String host = null;
        String portString = null;
        int port = 0;
        List<Dom> nodes = null;
        QueryNode qn = null;

        List<QueryNode> qns = null;
        for( Dom nodesDom: nodesDomList ) {
            gid = nodesDom.getAttributeValue("id");
            nodes = nodesDom.elements("node");
            qns = new ArrayList<QueryNode>(nodes.size());

            for( Dom nodeDom: nodes ) {
                id = nodeDom.getAttributeValue("id");
                host = nodeDom.getAttributeValue("host");
                portString = nodeDom.getAttributeValue("port");
                port = Integer.valueOf(portString);

                qn = new QueryNode(gid, id, host, port);
                qns.add(qn);
            }
            NODE_CONF_MAP.put(gid, qns);
        }
    }

    private String groupId;

    private String id;

    private String host;

    private int port;

    private QuerySlaveProtocol proxy;

    public QueryNode(String groupId, String id, String host, int port) {
        super();
        this.groupId = groupId;
        this.id = id;
        this.host = host;
        this.port = port;
        InetSocketAddress addr = new InetSocketAddress(host, port);
        try {
            this.proxy = (QuerySlaveProtocol) RPC.getProxy(
                    QuerySlaveProtocol.class, 1, addr, new Configuration());
            LOGGER.info("[QUERY-NODE] - " + this + " inited.");
        } catch (IOException e) {
            LOGGER.error(e);
        }
    }

    public String getGroupId() {
        return groupId;
    }

    public void setGroupId( String groupId ) {
        this.groupId = groupId;
    }

    public String getId() {
        return id;
    }

    public void setId( String id ) {
        this.id = id;
    }

    public QuerySlaveProtocol getProxy() {
        return proxy;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((groupId == null) ? 0 : groupId.hashCode());
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        return result;
    }

    @Override
    public boolean equals( Object obj ) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        QueryNode other = (QueryNode) obj;
        if (groupId == null) {
            if (other.groupId != null)
                return false;
        } else if (!groupId.equals(other.groupId))
            return false;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "QueryClient." + groupId + "." + id + "@" + host + ":" + port;
    }

}
