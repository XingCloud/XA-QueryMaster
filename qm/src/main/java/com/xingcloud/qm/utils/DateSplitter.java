package com.xingcloud.qm.utils;

import com.xingcloud.qm.enums.Interval;
import com.xingcloud.qm.exceptions.XParameterException;
import com.xingcloud.qm.plan.BeginEndDatePair;
import org.apache.commons.lang3.StringUtils;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import static com.xingcloud.basic.Constants.DEFAULT_TIME_ZONE;
import static com.xingcloud.basic.utils.DateUtils.*;
import static com.xingcloud.qm.enums.Interval.*;

public class DateSplitter {

  public static Date getNextCycle(Date currentDay, Interval interval) {
    if (currentDay == null) {
      return null;
    }
    Calendar c = Calendar.getInstance(DEFAULT_TIME_ZONE);
    c.setTime(currentDay);

    if (DAY.equals(interval)) {
      c.add(Calendar.DATE, 0);
    } else if (WEEK.equals(interval)) {
      c.add(Calendar.DATE, 6);
    } else if (MONTH.equals(interval)) {
      c.add(Calendar.MONTH, 1);
      c.add(Calendar.DATE, -1);
    } else {
      c.add(Calendar.DATE, 0);
    }
    return c.getTime();
  }

  public static List<BeginEndDatePair> split2Pairs(String beginDate, String endDate, Interval interval) throws
    ParseException {
    List<String> dates = split(beginDate, endDate, interval);
    return getBeginEndDatePairs(dates, interval);
  }

  public static List<BeginEndDatePair> getBeginEndDatePairs(List<String> dates, Interval interval) throws
    ParseException {
    if (dates == null || dates.isEmpty()) {
      return null;
    }
    List<BeginEndDatePair> pairs = new ArrayList<BeginEndDatePair>(dates.size());

    for (String currentDay : dates) {
      pairs.add(new BeginEndDatePair(currentDay, date2Short(getNextCycle(short2Date(currentDay), interval))));
    }

    return pairs;
  }

  public static List<String> split(String beginDate, String endDate, Interval interval) throws ParseException {
    if (StringUtils.isBlank(beginDate)) {
      throw new ParseException(beginDate, 0);
    }
    if (StringUtils.isBlank(endDate)) {
      throw new ParseException(beginDate, 0);
    }

    if (interval == null) {
      interval = DAY;
    }
    List<String> dates = new ArrayList<String>();
    if (beginDate.equals(endDate)) {
      dates.add(beginDate);
      return dates;
    }
    Calendar c = Calendar.getInstance(DEFAULT_TIME_ZONE);
    Date bDate = short2Date(beginDate);
    Date eDate = short2Date(nextDay(endDate));
    c.setTime(bDate);
    int timeUnit = -1;
    int amount = 1;

    if (DAY.equals(interval)) {
      timeUnit = Calendar.DATE;
    } else if (WEEK.equals(interval)) {
      timeUnit = Calendar.DATE;
      amount = 7;
    } else if (MONTH.equals(interval)) {
      timeUnit = Calendar.MONTH;
      c.clear(Calendar.DATE);
    } else if (HOUR.equals(interval)) {
      timeUnit = Calendar.DATE;
    } else {
      timeUnit = Calendar.DATE;
    }

    while (bDate.before(eDate)) {
      if (WEEK.equals(interval)) {
        dates.add(date2Short(getMonday(bDate)));
      } else if (MONTH.equals(interval)) {
        dates.add(date2Short(getFirstDayOfMonth(bDate)));
      } else {
        dates.add(date2Short(bDate));
      }
      c.add(timeUnit, amount);
      bDate = c.getTime();
    }
    return dates;
  }

  public static List<String> getQueryDateSlice(String beginDate, String endDate, Interval interval) throws
          XParameterException {
    try {
      return DateSplitter.split(beginDate, endDate, interval);
    } catch (ParseException e) {
      throw new XParameterException();
    }
  }

  public static void main(String[] args) throws ParseException, XParameterException {
    Interval i = Interval.WEEK;
    List<String> dates = split("2012-12-01", "2012-12-07", i);
    System.out.println(dates);
    System.out.println(getBeginEndDatePairs(dates, i));
  }
}
