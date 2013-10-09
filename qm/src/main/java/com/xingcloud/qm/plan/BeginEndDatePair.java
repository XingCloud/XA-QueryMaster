package com.xingcloud.qm.plan;

import com.google.common.base.Strings;

import java.io.Serializable;

public class BeginEndDatePair implements Serializable {

  private static final long serialVersionUID = 4643533411793477621L;
  protected String beginDate;
  protected String endDate;

  public BeginEndDatePair(String beginDate, String endDate) {
    super();
    this.beginDate = beginDate;
    this.endDate = endDate;
  }

  public BeginEndDatePair() {
    super();
  }

  public String getBeginDate() {
    return beginDate;
  }

  public void setBeginDate(String beginDate) {
    this.beginDate = beginDate;
  }

  public String getEndDate() {
    return endDate;
  }

  public void setEndDate(String endDate) {
    this.endDate = endDate;
  }

  @Override
  public String toString() {
    return "(" + beginDate + "," + endDate + ")";
  }

  public boolean isValid() {
    return (!Strings.isNullOrEmpty(beginDate)) && (!Strings.isNullOrEmpty(endDate)
    );
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((beginDate == null) ? 0 : beginDate.hashCode());
    result = prime * result + ((endDate == null) ? 0 : endDate.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    BeginEndDatePair other = (BeginEndDatePair) obj;
    if (beginDate == null) {
      if (other.beginDate != null)
        return false;
    } else if (!beginDate.equals(other.beginDate))
      return false;
    if (endDate == null) {
      if (other.endDate != null)
        return false;
    } else if (!endDate.equals(other.endDate))
      return false;
    return true;
  }

}
