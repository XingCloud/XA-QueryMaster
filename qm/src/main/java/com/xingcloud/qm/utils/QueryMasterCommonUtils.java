package com.xingcloud.qm.utils;

import static com.xingcloud.basic.Constants.SEPARATOR_CHAR_CACHE_CONTENT;
import static com.xingcloud.basic.Constants.SQL_COUNT;
import static com.xingcloud.basic.Constants.SQL_DISTINCT;
import static com.xingcloud.basic.Constants.SQL_FROM;
import static com.xingcloud.basic.Constants.SQL_GROUPBY;
import static com.xingcloud.basic.Constants.SQL_SELECT;
import static com.xingcloud.basic.Constants.SQL_STATEMENT_SEPARATOR;
import static com.xingcloud.basic.Constants.SQL_SUM;
import static com.xingcloud.qm.utils.QueryMasterConstant.COUNT;
import static com.xingcloud.qm.utils.QueryMasterConstant.GROUP;
import static com.xingcloud.qm.utils.QueryMasterConstant.INNER_HASH_KEY;
import static com.xingcloud.qm.utils.QueryMasterConstant.META_KEY;
import static com.xingcloud.qm.utils.QueryMasterConstant.SIZE_KEY;
import static com.xingcloud.qm.utils.QueryMasterConstant.SUM;
import static com.xingcloud.qm.utils.QueryMasterConstant.USER_NUM;

import com.google.common.base.Strings;
import com.xingcloud.cache.MappedXCache;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.log4j.Logger;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class QueryMasterCommonUtils {
  private static final Logger LOGGER = Logger.getLogger(QueryMasterCommonUtils.class);

  public static boolean hasGroupByKeyWord(String sql) {
    if (Strings.isNullOrEmpty(sql)) {
      return false;
    }
    int groupByIndex = sql.indexOf(SQL_GROUPBY);
    return groupByIndex >= 0;
  }

  public static MappedXCache converet2Cache(String key, MapWritable mw, int[] metaInfoArray) {

    Map<String, String> valueMap = new HashMap<String, String>(mw.size());
    mw.remove(SIZE_KEY);
    mw.remove(META_KEY);
    ArrayWritable aw = null;
    String[] valueArray = null;

    Long count = null;
    Long sum = null;
    Long userNum = null;
    String innerKey = null;
    StringBuilder sb = null;
    for (Entry<Writable, Writable> entry : mw.entrySet()) {
      aw = (ArrayWritable) entry.getValue();
      valueArray = aw.toStrings();

      for (int i = 0; i < metaInfoArray.length; i++) {
        switch (metaInfoArray[i]) {
          case COUNT:
            count = Long.valueOf(valueArray[i]);
            break;
          case SUM:
            sum = Long.valueOf(valueArray[i]);
            break;
          case USER_NUM:
            userNum = Long.valueOf(valueArray[i]);
            break;
          case GROUP:
            innerKey = valueArray[i];
            break;
          default:
            break;
        }
        if (Strings.isNullOrEmpty(innerKey)) {
          innerKey = INNER_HASH_KEY;
        }
      }
      sb = new StringBuilder();
      if (count != null) {
        sb.append(count);
      }
      sb.append(SEPARATOR_CHAR_CACHE_CONTENT);
      if (sum != null) {
        sb.append(sum);
      }
      sb.append(SEPARATOR_CHAR_CACHE_CONTENT);
      if (userNum != null) {
        sb.append(userNum);
      }
      sb.append(SEPARATOR_CHAR_CACHE_CONTENT);
      sb.append(1.0d);
      valueMap.put(innerKey, sb.toString());
    }
    MappedXCache mxc = new MappedXCache(key, valueMap, System.currentTimeMillis());
    return mxc;
  }

  @Test
  public static int[] parseSqlMeta(String sql, boolean hasGroupBy) {
    String selectFrom = sql.substring(SQL_SELECT.length(), sql.indexOf(SQL_FROM));
    String[] arr = selectFrom.split(SQL_STATEMENT_SEPARATOR);
    int length = hasGroupBy ? arr.length + 1 : arr.length;
    int[] result = new int[length];
    int cnt = 0;

    for (String s : arr) {
      s = s.trim();
      if (s.startsWith(SQL_COUNT) && !s.contains(SQL_DISTINCT)) {
        result[cnt] = COUNT;
      } else if (s.startsWith(SQL_SUM)) {
        result[cnt] = SUM;
      } else if (s.startsWith(SQL_COUNT) && s.contains(SQL_DISTINCT)) {
        result[cnt] = USER_NUM;
      } else {
        continue;
      }
      ++cnt;
    }
    if (hasGroupBy) {
      result[length - 1] = GROUP;
    }

    return result;
  }
}
