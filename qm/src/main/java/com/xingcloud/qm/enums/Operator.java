package com.xingcloud.qm.enums;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import java.rmi.activation.UnknownObjectException;
import java.util.EnumSet;
import java.util.Set;

public enum Operator {

  // Do not filter anything.
  ALL(),
  // >
  GT("gt", "$gt", ">"),
  // <
  LT("lt", "$lt", "<"),
  // >=
  GTE("gte", "$gte", ">="),
  // <=
  LTE("lte", "$lte", "<="),
  // ==
  EQ("eq", "eq", "="),
  // !=
  NE("ne", "$ne", "!="),
  IN("in", "in", "in"),
  // between ... and ...[x, y]
  BETWEEN;

  private static final Set<Operator> SET = EnumSet.allOf(Operator.class);

  public static final BiMap<Operator, String> OPERATOR_COMMON_KEYWORDS_BIMAP = HashBiMap.create();
  public static final BiMap<Operator, String> OPERATOR_MONGO_KEYWORDS_BIMAP = HashBiMap.create();

  static {
    String keyword;
    for (Operator operator : SET) {
      keyword = operator.getCommonKeyword();
      if (keyword != null) {
        OPERATOR_COMMON_KEYWORDS_BIMAP.put(operator, keyword);
      }
      keyword = operator.getMongoKeyword();
      if (keyword != null) {
        OPERATOR_MONGO_KEYWORDS_BIMAP.put(operator, keyword);
      }
    }
  }

  private String commonKeyword;

  private String mongoKeyword;

  private String mathOperator;

  private Operator() {
  }

  private Operator(String commonKeyword, String mongoKeyword, String mathOperator) {
    this.commonKeyword = commonKeyword;
    this.mongoKeyword = mongoKeyword;
    this.mathOperator = mathOperator;
  }

  public String getCommonKeyword() {
    return commonKeyword;
  }

  public void setCommonKeyword(String commonKeyword) {
    this.commonKeyword = commonKeyword;
  }

  public String getMongoKeyword() {
    return mongoKeyword;
  }

  public void setMongoKeyword(String mongoKeyword) {
    this.mongoKeyword = mongoKeyword;
  }

  public String getMathOperator() {
    return mathOperator;
  }

  public void setMathOperator(String mathOperator) {
    this.mathOperator = mathOperator;
  }

  public static String mongo2Math(String mongoKeyword) throws UnknownObjectException {
    try {
      return OPERATOR_MONGO_KEYWORDS_BIMAP.inverse().get(mongoKeyword).getMathOperator();
    } catch (Exception e) {
      throw new UnknownObjectException("Unknown mongo operation key word - " + mongoKeyword);
    }
  }

  public static String common2Math(String commonKeyword) throws UnknownObjectException {
    try {
      return OPERATOR_COMMON_KEYWORDS_BIMAP.inverse().get(commonKeyword).getMathOperator();
    } catch (Exception e) {
      throw new UnknownObjectException("Unknown common operation key word - " + commonKeyword);
    }
  }

  public static void main(String[] args) throws UnknownObjectException {
    Set<Operator> set= EnumSet.allOf(Operator.class);
    for(Operator o: set){
      System.out.println(o.getMathOperator());
    }
  }

}
