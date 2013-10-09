package com.xingcloud.qm.plan;

import org.apache.commons.lang.ArrayUtils;
import org.apache.drill.common.expression.LogicalExpression;

import java.util.HashMap;
import java.util.Map;

import static org.apache.drill.common.util.Selections.*;

/**
 * User: Z J Wu Date: 13-8-26 Time: 下午4:35 Package: com.xingcloud.webinterface.plan
 */
public class RowkeyScanFilterDescriptor extends ScanFilterDescriptor {

  private LogicalExpression[] expressions;
  private LogicalExpression[] originalEventLevelMap;

  public RowkeyScanFilterDescriptor(ScanFilterType type, LogicalExpression[] expressions,
                                    LogicalExpression[] originalEventLevelMap) {
    super(type);
    this.expressions = expressions;
    this.originalEventLevelMap = originalEventLevelMap;
  }

  public LogicalExpression[] getExpressions() {
    return expressions;
  }

  public void setExpressions(LogicalExpression[] expressions) {
    this.expressions = expressions;
  }

  public LogicalExpression[] getOriginalEventLevelMap() {
    return originalEventLevelMap;
  }

  public void setOriginalEventLevelMap(LogicalExpression[] originalEventLevelMap) {
    this.originalEventLevelMap = originalEventLevelMap;
  }

  @Override
  public Map<String, Object> toMap() {
    Map<String, Object> map = new HashMap<String, Object>(2);
    map.put(SELECTION_KEY_WORD_FILTER_TYPE, this.type);
    map.put(SELECTION_KEY_WORD_ROWKEY_INCLUDES, this.expressions);
    if (ArrayUtils.isNotEmpty(this.originalEventLevelMap)) {
      map.put(SELECTION_KEY_WORD_ROWKEY_EVENT_MAPPING, this.originalEventLevelMap);
    }
    return map;
  }
}
