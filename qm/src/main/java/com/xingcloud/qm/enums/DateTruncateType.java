package com.xingcloud.qm.enums;

public enum DateTruncateType {
  PASS(0), TRIM(1), KILL(2);

  private int level;

  private DateTruncateType(int level) {
    this.level = level;
  }

  public int getLevel() {
    return level;
  }

  public void setLevel(int level) {
    this.level = level;
  }

}
