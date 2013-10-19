package com.xingcloud.qm.result;

import java.util.*;

public class ResultTable implements Map<String, ResultRow> {
  
  Map<String, ResultRow> resultLines;


  public ResultTable() {
    resultLines = new HashMap<>();
  }

  public ResultTable(Map<String, ResultRow> resultLines) {
    this.resultLines = resultLines;
  }

  @Override
  public int size() {
    return resultLines.size();
  }

  @Override
  public boolean isEmpty() {
    return resultLines.isEmpty();
  }

  @Override
  public boolean containsKey(Object key) {
    return resultLines.containsKey(key);
  }

  @Override
  public boolean containsValue(Object value) {
    return resultLines.containsValue(value);
  }

  @Override
  public ResultRow get(Object key) {
    return resultLines.get(key);
  }

  @Override
  public ResultRow put(String key, ResultRow value) {
    return resultLines.put(key, value);
  }

  @Override
  public ResultRow remove(Object key) {
    return resultLines.remove(key);
  }

  @Override
  public void putAll(Map<? extends String, ? extends ResultRow> m) {
    resultLines.putAll(m);
  }

  @Override
  public void clear() {
    resultLines.clear();
  }

  @Override
  public Set<String> keySet() {
    return resultLines.keySet();
  }

  @Override
  public Collection<ResultRow> values() {
    return resultLines.values();
  }

  @Override
  public Set<Entry<String,ResultRow>> entrySet() {
    return resultLines.entrySet();
  }

  @Override
  public boolean equals(Object o) {
    return resultLines.equals(o);
  }

  @Override
  public int hashCode() {
    return resultLines.hashCode();
  }

  public Map<String, Number[]> toCacheValue() {
    Map<String, Number[]> out = new HashMap<>();
    for (Entry<String, ResultRow> entry : this.entrySet()) {
      String key = entry.getKey();
      ResultRow row = entry.getValue();
      out.put(key, new Number[]{row.count, row.sum, row.userNum, row.sampleRate});
    }
    return out;
  }

  public void add(ResultTable another) {
    Map<String, ResultRow> anoRl = another.resultLines;
    for (Map.Entry<String, ResultRow> entry : anoRl.entrySet()) {
      String key = entry.getKey();
      ResultRow rr = entry.getValue();
      ResultRow rrCurrent = resultLines.get(key);
      if (rrCurrent != null) {
        rrCurrent.add(rr);
      } else {
        resultLines.put(key, rr);
      }
    }
  }

  public void setSampleRate(double sampleRate) {
    for (Entry<String, ResultRow> entry : resultLines.entrySet()) {
      ResultRow rr = entry.getValue();
      rr.sampleRate = sampleRate;
    }
  }
}
