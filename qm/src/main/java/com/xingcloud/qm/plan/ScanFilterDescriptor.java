package com.xingcloud.qm.plan;

import java.util.Map;

/**
 * User: Z J Wu Date: 13-8-26 Time: 下午4:34 Package: com.xingcloud.webinterface.plan
 */
public abstract class ScanFilterDescriptor {
  protected ScanFilterType type;

  public ScanFilterType getType() {
    return type;
  }

  public void setType(ScanFilterType type) {
    this.type = type;
  }

  public ScanFilterDescriptor(ScanFilterType type) {
    this.type = type;
  }

  public abstract Map<String, Object> toMap();
}
