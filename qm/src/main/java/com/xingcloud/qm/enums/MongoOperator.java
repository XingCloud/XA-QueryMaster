package com.xingcloud.qm.enums;

public enum MongoOperator {
  LTE("$lte"), LT("$lt"), GTE("$gte"), GT("$gt");

  private String operator;

  private MongoOperator(String operator) {
    this.operator = operator;
  }

  public String getOperator() {
    return operator;
  }

  public void setOperator(String operator) {
    this.operator = operator;
  }

}
