package com.xingcloud.qm.plan;

import com.xingcloud.meta.ByteUtils;
import com.xingcloud.qm.enums.KeyPartParameterType;


/**
 * User: Z J Wu Date: 13-8-5 Time: 下午5:17 Package: com.xingcloud.webinterface.plan
 */
public class KeyPartParameter {
  private byte[] parameterValue1;
  private byte[] parameterValue2;
  private KeyPartParameterType parameterValueType;

  public KeyPartParameter(byte[] parameterValue1, KeyPartParameterType parameterValueType) {
    this.parameterValue1 = parameterValue1;
    this.parameterValueType = parameterValueType;
  }

  public KeyPartParameter(byte[] parameterValue1, byte[] parameterValue2, KeyPartParameterType parameterValueType) {
    this.parameterValue1 = parameterValue1;
    this.parameterValue2 = parameterValue2;
    this.parameterValueType = parameterValueType;
  }

  public boolean isSingle() {
    return getParameterValueType().equals(KeyPartParameterType.SINGLE);
  }

  public byte[] getParameterValue1() {
    return parameterValue1;
  }

  public byte[] getParameterValue2() {
    return parameterValue2;
  }

  public KeyPartParameterType getParameterValueType() {
    return parameterValueType;
  }

  public static KeyPartParameter buildSingleKey(byte[] parameterValue) {
    return new KeyPartParameter(parameterValue, KeyPartParameterType.SINGLE);
  }

  public static KeyPartParameter buildRangeKey(byte[] parameterValue1, byte[] parameterValue2) {
    return new KeyPartParameter(parameterValue1, parameterValue2, KeyPartParameterType.RANGE);
  }

  @Override public String toString() {
    return "KPP(" + ByteUtils.toStringBinary(this.parameterValue1) + " - " + ByteUtils
      .toStringBinary(this.parameterValue2) + ")";
  }
}
