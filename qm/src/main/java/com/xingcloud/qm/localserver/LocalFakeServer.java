package com.xingcloud.qm.localserver;

import static com.xingcloud.qm.utils.QueryMasterCommonUtils.hasGroupByKeyWord;
import static com.xingcloud.qm.utils.QueryMasterCommonUtils.parseSqlMeta;
import static com.xingcloud.qm.utils.QueryMasterConstant.SIZE_KEY;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomStringUtils.randomNumeric;

import com.xingcloud.basic.remote.QuerySlaveProtocol;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;

import java.io.IOException;

public class LocalFakeServer implements QuerySlaveProtocol {

  @Override
  public long getProtocolVersion(String arg0, long arg1) throws IOException {
    return 1;
  }

  @Override
  public MapWritable query(String sql) throws Exception {
    if (hasGroupByKeyWord(sql)) {
      return queryProjectionAggr(sql);
    }
    return queryAggr(sql);
  }

  public static MapWritable queryProjectionAggr(String sql) {
    int[] metaInfoArray = parseSqlMeta(sql, true);
    MapWritable mw;
    LongWritable lw;
    int randomCnt;
    ArrayWritable aw;
    String[] rowData;
    mw = new MapWritable();
    randomCnt = 3;
    mw.put(SIZE_KEY, new LongWritable(randomCnt));

    for (int j = 0; j < randomCnt; j++) {
      lw = new LongWritable(j);
      rowData = new String[metaInfoArray.length];
      for (int k = 0; k < rowData.length - 1; k++) {
        rowData[k] = randomNumeric(1);
      }
      rowData[rowData.length - 1] = randomAlphabetic(1);
      aw = new ArrayWritable(rowData);
      mw.put(lw, aw);
    }
    // printRelations(mw);
    return mw;
  }

  public static MapWritable queryAggr(String sql) {
    int[] metaInfoArray = parseSqlMeta(sql, false);
    MapWritable mw;
    LongWritable lw;
    ArrayWritable aw;
    String[] rowData;
    mw = new MapWritable();
    mw.put(SIZE_KEY, new LongWritable(1));

    for (int j = 0; j < 1; j++) {
      lw = new LongWritable(j);
      rowData = new String[metaInfoArray.length];
      for (int k = 0; k < rowData.length; k++) {
        rowData[k] = randomNumeric(1);
      }
      aw = new ArrayWritable(rowData);
      mw.put(lw, aw);
    }
    // printRelations(mw);
    return mw;
  }
}
