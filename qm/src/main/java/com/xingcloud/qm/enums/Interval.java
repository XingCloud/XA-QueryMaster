package com.xingcloud.qm.enums;

public enum Interval {

  DAY(1f), WEEK(7f), MONTH(30f), HOUR(0.041f), MIN5(0.003f),
  PERIOD(Float.MAX_VALUE);

  private float days;

  private Interval() {
  }

  private Interval(float days) {
    this.days = days;
  }

  public float getDays() {
    return days;
  }

  public void setDays(float days) {
    this.days = days;
  }

  public static void main(String[] args) {
    for (Interval interval : Interval.values()) {
      System.out.println(interval.getDays());
    }
  }

}
