package com.xingcloud.qm.result;

public class ResultRow {
  public long count;
  public long sum;
  public long userNum;
  public double sampleRate = 1;

  public ResultRow() {
  }

  public ResultRow(long count, long sum, long userNum) {
    this.count = count;
    this.sum = sum;
    this.userNum = userNum;
  }

  public ResultRow(long count, long sum, long userNum, double sampleRate) {
    this.count = count;
    this.sum = sum;
    this.userNum = userNum;
    this.sampleRate = sampleRate;
  }
}
