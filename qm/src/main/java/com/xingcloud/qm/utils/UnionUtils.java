package com.xingcloud.qm.utils;

import static com.xingcloud.qm.utils.QueryMasterConstant.GROUP;
import static com.xingcloud.qm.utils.QueryMasterConstant.SIZE_KEY;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class UnionUtils {
  public static RowEntry<String, Object[]> toRowEntry(String[] row, int[] metaInfoArray) {
    if (ArrayUtils.isEmpty(row)) {
      return null;
    }
    if (ArrayUtils.isEmpty(metaInfoArray)) {
      return null;
    }

    String k = null;
    StringBuilder sb = new StringBuilder();
    String cell = null;
    Object[] valueArray = null;

    for (int i = 0; i < row.length; i++) {
      cell = row[i];
      if (valueArray == null) {
        valueArray = new Object[metaInfoArray.length];
      }

      if (metaInfoArray[i] == GROUP) {
        sb.append(cell);
        valueArray[i] = cell;
      } else {
        valueArray[i] = Long.parseLong(cell);
      }
    }
    k = sb.toString();
    RowEntry<String, Object[]> re = new RowEntry<String, Object[]>(k, valueArray);
    return re;
  }

  public static void mergeArray(Object[] a1, Object[] a2, int[] metaInfoArray) {
    if (ArrayUtils.isEmpty(a2)) {
      return;
    }
    if (ArrayUtils.isEmpty(a1)) {
      a1 = a2;
    }
    for (int i = 0; i < metaInfoArray.length; i++) {
      if (metaInfoArray[i] == GROUP) {
        continue;
      }
      a1[i] = (Long) a1[i] + (Long) a2[i];
    }
  }

  @Deprecated
  /**
   * Not supported in current version.
   * @param parts
   * @return
   */
  public static MapWritable unionProjection(List<MapWritable> parts) {
    MapWritable union = new MapWritable();
    int allSize = 0;
    long cnt = 0;
    LongWritable sizeWritable = null;
    Writable row = null;
    LongWritable longWritable = new LongWritable();
    long size = 0;
    for (MapWritable part : parts) {
      sizeWritable = (LongWritable) part.get(SIZE_KEY);
      if (sizeWritable == null) {
        continue;
      }
      size = sizeWritable.get();
      if (size == 0) {
        continue;
      }

      allSize += size;
      for (long i = 0; i < size; i++) {
        longWritable = new LongWritable(i);
        row = part.get(longWritable);
        if (row == null) {
          continue;
        }
        longWritable = new LongWritable(cnt);
        union.put(longWritable, row);
        ++cnt;
      }
    }
    union.put(SIZE_KEY, new LongWritable(allSize));
    return union;
  }

  public static MapWritable unionAggr(List<MapWritable> parts) {
    MapWritable union = new MapWritable();
    LongWritable sizeWritable = null;
    ArrayWritable row = null;
    long size = 0;
    String[] stringArray = null;
    long[] l = null;
    for (MapWritable part : parts) {
      sizeWritable = (LongWritable) part.get(SIZE_KEY);
      if (sizeWritable == null) {
        continue;
      }
      size = sizeWritable.get();
      if (size == 0) {
        continue;
      }

      row = (ArrayWritable) part.get(new LongWritable(0));
      stringArray = row.toStrings();
      if (l == null) {
        l = new long[stringArray.length];
      }
      for (int i = 0; i < stringArray.length; i++) {
        l[i] += Long.valueOf(stringArray[i]);
      }
    }
    stringArray = new String[l.length];
    for (int i = 0; i < l.length; i++) {
      stringArray[i] = String.valueOf(l[i]);
    }
    row = new ArrayWritable(stringArray);
    union.put(SIZE_KEY, new LongWritable(1));
    union.put(new LongWritable(0), row);
    return union;
  }

  public static MapWritable unionProjectionAggr(List<MapWritable> parts, int[] metaInfoArray) {
    MapWritable union = new MapWritable();

    LongWritable longWritable = null;

    long size = 0;
    Map<String, Object[]> map = new HashMap<String, Object[]>();
    ArrayWritable row = null;
    RowEntry<String, Object[]> re = null;
    String k = null;
    Object[] v = null;

    for (MapWritable part : parts) {
      longWritable = (LongWritable) part.get(SIZE_KEY);
      if (longWritable == null) {
        continue;
      }
      size = longWritable.get();
      if (size == 0) {
        continue;
      }
      for (int i = 0; i < size; i++) {
        row = (ArrayWritable) part.get(new LongWritable(i));
        re = toRowEntry(row.toStrings(), metaInfoArray);
        k = re.getKey();
        v = map.get(k);
        if (v == null) {
          map.put(k, re.getValue());
          continue;
        }
        mergeArray(v, re.getValue(), metaInfoArray);
      }
    }
    union.put(SIZE_KEY, new LongWritable(map.size()));

    long l = 0;
    String[] stringArray = null;
    for (Entry<String, Object[]> entry : map.entrySet()) {
      v = entry.getValue();

      stringArray = new String[v.length];
      for (int i = 0; i < v.length; i++) {
        stringArray[i] = String.valueOf(v[i]);
      }
      row = new ArrayWritable(stringArray);
      union.put(new LongWritable(l), row);
      ++l;
    }
    return union;
  }

  /*
   *
   * @param parts
   * @param infoArray
   * @return
   * 3 kinds
   *  1. Only relations select a, b, c from
   *  2. Only aggrs select count(1), sum(a) from
   *  3. Both relations and aggrs select a, count ( distinct b) from
   */
  public static MapWritable union(List<MapWritable> parts, boolean hasGroupBy, int[] metaInfoArray) {
    if (ArrayUtils.isEmpty(metaInfoArray)) {
      return null;
    }
    if (CollectionUtils.isEmpty(parts)) {
      return null;
    }
    MapWritable union = hasGroupBy ? unionProjectionAggr(parts, metaInfoArray) : unionAggr(parts);
    return union;
  }
}
