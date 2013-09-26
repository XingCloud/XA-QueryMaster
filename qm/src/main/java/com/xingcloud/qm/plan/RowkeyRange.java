package com.xingcloud.qm.plan;

import org.apache.commons.lang3.ArrayUtils;

import static com.xingcloud.meta.ByteUtils.toStringBinary;

/**
 * User: Z J Wu Date: 13-8-5 Time: 下午5:12 Package: com.xingcloud.webinterface.plan
 */
public class RowkeyRange {
  private String startKey;
  private String endKey;

  private RowkeyRange(String startKey, String endKey) {
    this.startKey = startKey;
    this.endKey = endKey;
  }

  public String getStartKey() {
    return startKey;
  }

  public String getEndKey() {
    return endKey;
  }

  private static boolean validate(byte[] startKey, byte[] endKey) {
    if (ArrayUtils.isEmpty(startKey) || ArrayUtils.isEmpty(endKey)) {
      return false;
    }
    return true;
  }

  public static RowkeyRange create(byte[] startKey, byte[] endKey) {
    if (validate(startKey, endKey)) {
      return new RowkeyRange(toStringBinary(startKey), toStringBinary(endKey));
    }
    throw new IllegalArgumentException("Start key or end key can not be empty - " + startKey + " - " + endKey);
  }

  @Override
  public String toString() {
    return "PK(" + startKey + ',' + endKey + ")";
  }
}
