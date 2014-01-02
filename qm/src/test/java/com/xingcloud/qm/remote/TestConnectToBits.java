package com.xingcloud.qm.remote;

import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 1/2/14
 * Time: 11:51 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestConnectToBits {
  @Test
  public void test() {
    ConnectToBits connectToBits = new ConnectToBits();
    Thread thread = new Thread(connectToBits);
    thread.start();
  }

}
