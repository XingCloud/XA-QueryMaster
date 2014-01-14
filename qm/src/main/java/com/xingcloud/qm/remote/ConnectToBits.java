package com.xingcloud.qm.remote;

import net.sf.ehcache.util.NamedThreadFactory;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.concurrent.*;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 1/2/14
 * Time: 11:10 AM
 * To change this template use File | Settings | File Templates.
 */
public class ConnectToBits implements Runnable {
  private static Log LOG= LogFactory.getLog(ConnectToBits.class);
  private ConnectionState connectionState;
  private ExecutorService service=new ThreadPoolExecutor(QueryNode.getNodes().length,QueryNode.getNodes().length,30, TimeUnit.SECONDS,
                                  new ArrayBlockingQueue<Runnable>(QueryNode.getNodes().length),new NamedThreadFactory("connect"));
  public ConnectToBits(){
     detectConnection();
  }
  @Override
  public void run() {
    while(true){
      try {
        Thread.sleep(10*1000);
      } catch (InterruptedException e) {
        e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
      }
      detectAndConnect();
    }
  }

  private void detectConnection(){
    for(QueryNode node: QueryNode.getNodes()){
      if(node.getConnectionState()==ConnectionState.disconnection){
        connectionState=ConnectionState.disconnection;
        return;
      }
    }
    connectionState=ConnectionState.active;
    LOG.info("detect Connection. Result is "+connectionState);
  }

  private void detectAndConnect(){
    int unActiveNum=0;
    for(QueryNode node: QueryNode.getNodes()){
      if(node.getConnectionState()==ConnectionState.disconnection){
          service.submit(new ConnectToOneBit(node));
          unActiveNum++;
      }else if(node.getConnectionState()==ConnectionState.connecting){
        unActiveNum++;
        continue;
      }
    }
    if(unActiveNum==0)
      connectionState=ConnectionState.active;
    else
      connectionState=ConnectionState.connecting;
    LOG.info("connection state is "+connectionState);
  }

  public static void main(String[] args){
    ConnectToBits connectToBits = new ConnectToBits();
    Thread thread = new Thread(connectToBits);
    thread.start();
  }
}
