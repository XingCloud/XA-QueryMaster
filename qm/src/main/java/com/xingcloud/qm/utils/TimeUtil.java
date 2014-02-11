package com.xingcloud.qm.utils;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * User: liuxiong
 * Date: 13-7-25
 * Time: 下午5:18
 */
public class TimeUtil {

  public static final String TIMEZONE = "GMT+8";
  public static final TimeZone TZ = TimeZone.getTimeZone(TIMEZONE);


  public static String getTime(long timestamp) {
    final SimpleDateFormat DF = new SimpleDateFormat("HH:mm:ss");
    DF.setTimeZone(TZ);
    Date date = new Date(timestamp);
    return DF.format(date);
  }


  public static void main(String[] args) {
    long timestamp = System.currentTimeMillis();
    System.out.println(getTime(timestamp));
    for (int i = 0; i < 100000; i++) {
      getTime(timestamp);
    }
    System.out.println(System.currentTimeMillis() - timestamp);
  }

}
