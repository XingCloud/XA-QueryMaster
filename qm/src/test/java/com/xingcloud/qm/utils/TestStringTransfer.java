package com.xingcloud.qm.utils;

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 10/9/13
 * Time: 3:49 PM
 * To change this template use File | Settings | File Templates.
 */
public class TestStringTransfer {
  @Test
  public void transeferBytes(){
      String src="\\\\x";
      byte[] targetBytes= Bytes.toBytesBinary(src);
  }
}
