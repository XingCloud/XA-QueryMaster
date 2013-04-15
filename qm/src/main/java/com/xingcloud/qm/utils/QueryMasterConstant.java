package com.xingcloud.qm.utils;

import static com.xingcloud.basic.Constants.SQL_GROUPBY;

import org.apache.hadoop.io.Text;

public class QueryMasterConstant {

  public static final String INNER_HASH_KEY = "XValue";
  public static final int SQL_GROUPBY_LENGTH = SQL_GROUPBY.length();
  public static final Text SIZE_KEY = new Text("size");
  public static final Text META_KEY = new Text("meta");

  public static final int COUNT = 0;
  public static final int SUM = 1;
  public static final int USER_NUM = 2;
  public static final int GROUP = -1;
}
