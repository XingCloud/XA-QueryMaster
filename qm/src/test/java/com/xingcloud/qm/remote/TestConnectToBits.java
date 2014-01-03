package com.xingcloud.qm.remote;

import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 1/2/14
 * Time: 11:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestConnectToBits {
  @Test
  public void test() throws InterruptedException {
    ExecutorService service = new ThreadPoolExecutor(1,1,30, TimeUnit.MINUTES,new ArrayBlockingQueue<Runnable>(2));
    ConnectToBits connectToBits = new ConnectToBits();
    service.execute(connectToBits);
    service.shutdown();
    service.awaitTermination(20,TimeUnit.MINUTES);
  }

}
