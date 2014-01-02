package com.xingcloud.qm.remote;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 1/2/14
 * Time: 11:02 AM
 * To change this template use File | Settings | File Templates.
 */
public class ConnectToOneBit implements Runnable {
  private static Log LOG= LogFactory.getLog(ConnectToOneBit.class);
  private QueryNode node;
  public ConnectToOneBit(QueryNode node){
     this.node=node;
  }
  @Override
  public void run() {
    node.setConnectionState(ConnectionState.connecting);
    LOG.info("connecting node "+node.getId());
    node.reconnect();
    if(node.getDrillClient().isActive())
      node.setConnectionState(ConnectionState.active);
    else
      node.setConnectionState(ConnectionState.disconnection);
  }
}
