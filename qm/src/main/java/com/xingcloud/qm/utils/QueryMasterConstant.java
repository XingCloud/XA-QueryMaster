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

  public static final int TOTAL_BUCKET_NUM = 256;
  public static final int[] SAMPLING_ARRAY = {1, 2, 4, 8, 16, 32, 64, 64, 65};
  public static final int SAMPLING_THRESHOLD = 3200;

  public static enum STORAGE_ENGINE {
    mysql, hbase
  };
}
