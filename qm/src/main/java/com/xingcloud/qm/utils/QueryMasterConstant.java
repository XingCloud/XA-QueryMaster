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

  public static final String EXPRESSION = "expression";
  public static final String EXPR = "expr";
  public static final String REF = "ref";
  public static final String OR = "or";

  //事件层级名称
  public static final String EVENT0 = "event0";
  public static final String EVENT1 = "event1";
  public static final String EVENT2 = "event2";
  public static final String EVENT3 = "event3";
  public static final String EVENT4 = "event4";
  public static final String EVENT5 = "event5";

  public static final String DATE = "date";
  public static final String MIN_UID = "\\x00\\x00\\x00\\x00\\x00";
  public static final byte[] MIN_UID_BYTES = {0,0,0,0,0};
  public static final String MAX_UID = "\\xFF\\xFF\\xFF\\xFF\\xFF";
  public static final byte[] MAX_UID_BYTES = {-1,-1,-1,-1,-1};

  public static final String NA_START_KEY = "\\x00\\x00\\x00"+"\\xFF\\x00\\x00\\x00\\x00\\x00";
  public static final String NA_END_KEY = "\\x00\\x00\\x00"+"\\xFF\\xFF\\xFF\\xFF\\xFF\\xFF";

  public static final String HBASE = "hbase";
  public static final String DEU = "deu";

  public static final String XFF = "\\xFF";
  public static final byte[] XFF_BYTES = {-1};

  public static enum STORAGE_ENGINE {
    mysql, hbase
  };
  public static final int[] SAMPLING_ARRAY = {1, 2, 4, 8, 16, 32, 64, 64, 65};
  public static final int TOTAL_BUCKET_NUM = 256;

  public static final int SAMPLING_THRESHOLD = 3200;


}
