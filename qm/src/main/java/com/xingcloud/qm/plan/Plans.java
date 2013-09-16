package com.xingcloud.qm.plan;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.events.*;
import com.xingcloud.memcache.MemCacheException;
import com.xingcloud.memcache.XMemCacheManager;
import com.xingcloud.mysql.PropType;
import com.xingcloud.mysql.UserProp;
import static com.xingcloud.qm.cache.MemoryCachedObjects.*;
import com.xingcloud.qm.cache.MemoryCachedObjects;
import com.xingcloud.qm.enums.Interval;
import com.xingcloud.qm.enums.Operator;
import com.xingcloud.qm.exceptions.PlanException;
import com.xingcloud.qm.utils.DateSplitter;
import com.xingcloud.qm.utils.UserPropertiesInfoManager;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.logical.data.*;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.*;

import static com.xingcloud.basic.utils.DateUtils.dateAdd;
import static com.xingcloud.meta.ByteUtils.toBytes;
import static org.apache.drill.common.util.DrillConstants.SE_HBASE;
import static org.apache.drill.common.util.DrillConstants.SE_MYSQL;
import static org.apache.drill.common.util.FieldReferenceBuilder.buildColumn;
import static org.apache.drill.common.util.FieldReferenceBuilder.buildTable;

/**
 * User: Z J Wu Date: 13-8-5 Time: 下午2:58 Package: com.xingcloud.webinterface.utils
 */
public class Plans {
  public static final byte MIN_EVENT_BYTE = 0;
  public static final String KEY_WORD_GENERIC_EVENT_TABLE_NAME = "generic_project";
  public static final String KEY_WORD_PROJECTIONS = "projections";
  public static final String KEY_WORD_UID = "uid";
  public static final String KEY_WORD_EVENT = "event";
  public static final String KEY_WORD_USER = "user";
  public static final String KEY_WORD_VALUE = "value";
  public static final String KEY_WORD_VAL = "val";
  public static final String KEY_WORD_DIMENSION = "dimension";
  public static final String KEY_WORD_SGMT = "sgmt";
  public static final String KEY_WORD_TIMESTAMP = "timestamp";

  public static FunctionRegistry DEFAULT_FUNCTION_REGISTRY = new FunctionRegistry(DrillConfig.create());

  public static DrillConfig DEFAULT_DRILL_CONFIG = DrillConfig.create();

  @Deprecated
  public static short propertyString2TinyInt(String projectId, String propName) throws Exception {
    UserProp up = UserPropertiesInfoManager.getInstance().getUserProp(projectId, propName);
    return (short) 1;
  }

  public static String toEventTableName(String projectId) {
    return "deu_" + projectId;
  }

  public static String toUserIndexTableName(String projectId) {
    return "property_" + projectId + "_index";
  }

  public static String toUserMysqlTableName(String projectId, String propName) {
    return projectId + "." + propName;
  }

  private static boolean usingDateFunc(String projectId, String propName) throws PlanException {
    UserProp up;
    try {
      up = UserPropertiesInfoManager.getInstance().getUserProp(projectId, propName);
    } catch (Exception e) {
      throw new PlanException(e);
    }
    PropType pt = up.getPropType();
    return PropType.sql_datetime.equals(pt);
  }

  private static boolean usingBeginSuffix(Operator op) {
    return Operator.GTE.equals(op);
  }

  private static String object2String(boolean transformDate, boolean beginSuffix, Object o) {
    if (o instanceof Number || o instanceof Boolean) {
      return o.toString();
    } else {
      if (transformDate) {
        String dateString = o.toString().replace("-", "");
        return beginSuffix ? dateString + "000000" : dateString + "235959";
      } else {
        return "'" + o.toString() + "'";
      }
    }
  }

  private static LogicalOperator getSingleMysqlSegmentScan(String projectId, String propertyName,
                                                           Map<Operator, Object> valueMap) throws PlanException {
    if (MapUtils.isEmpty(valueMap)) {
      throw new PlanException("Empty segment value");
    }

    boolean transformDate = usingDateFunc(projectId, propertyName);
    Object valueObject;

    if (transformDate && valueMap.containsKey(Operator.EQ)) {
      valueObject = valueMap.get(Operator.EQ);
      valueMap.remove(Operator.EQ);
      valueMap.put(Operator.GTE, valueObject);
      valueMap.put(Operator.LTE, valueObject);
    }

    String valString = "val";
    Map<String, Object> selectionMap = new HashMap<String, Object>(3);
    String table = toUserMysqlTableName(projectId, propertyName);
    selectionMap.put("table", table);
    FieldReference fr = buildColumn(KEY_WORD_UID);
    NamedExpression[] projections = new NamedExpression[]{new NamedExpression(fr, fr)};
    selectionMap.put(KEY_WORD_PROJECTIONS, projections);

    Iterator<Map.Entry<Operator, Object>> it = valueMap.entrySet().iterator();
    Map.Entry<Operator, Object> entry = it.next();

    Operator operator = entry.getKey();
    Object conditionVal = entry.getValue();

    StringBuilder filterSB = new StringBuilder();
    boolean usingBeginSuffix = usingBeginSuffix(operator);
    filterSB.append(valString);
    filterSB.append(operator.getMathOperator());
    filterSB.append(object2String(transformDate, usingBeginSuffix, conditionVal));

    for (; ; ) {
      if (!it.hasNext()) {
        break;
      }
      entry = it.next();
      operator = entry.getKey();
      usingBeginSuffix = usingBeginSuffix(operator);
      conditionVal = entry.getValue();
      filterSB.append(" and ");
      filterSB.append(valString);
      filterSB.append(operator.getMathOperator());
      filterSB.append(object2String(transformDate, usingBeginSuffix, conditionVal));
    }
    selectionMap.put("filter", filterSB.toString());
    ObjectMapper mapper = DEFAULT_DRILL_CONFIG.getMapper();
    String str;
    try {
      str = mapper.writeValueAsString(selectionMap);
      Scan scan = new Scan(SE_MYSQL, mapper.readValue(str, JSONOptions.class), buildTable(KEY_WORD_USER));
      scan.setMemo("Scan(Table=" + table + ", Prop=" + propertyName + ", Val=" + valueMap + ")");
      return scan;
    } catch (Exception e) {
      throw new PlanException(e);
    }
  }

  public static LogicalOperator getChainedMysqlSegmentScan(String projectId, List<LogicalOperator> operators,
                                                           Map<String, Map<Operator, Object>> segmentMap) throws
    PlanException {
    if (MapUtils.isEmpty(segmentMap)) {
      throw new PlanException("Empty segment");
    }
    Iterator<Map.Entry<String, Map<Operator, Object>>> it = segmentMap.entrySet().iterator();
    Map.Entry<String, Map<Operator, Object>> entry = it.next();
    String propertyName = entry.getKey();
    Map<Operator, Object> singleSegmentMap = entry.getValue();
    LogicalOperator lo1 = getSingleMysqlSegmentScan(projectId, propertyName, singleSegmentMap), lo2;
    operators.add(lo1);

    Join join;
    JoinCondition[] joinConditions;
    for (; ; ) {
      if (!it.hasNext()) {
        break;
      }
      entry = it.next();
      propertyName = entry.getKey();
      singleSegmentMap = entry.getValue();
      lo2 = getSingleMysqlSegmentScan(projectId, propertyName, singleSegmentMap);
      operators.add(lo2);

      joinConditions = new JoinCondition[1];
      joinConditions[0] = new JoinCondition("==", buildColumn(KEY_WORD_UID), buildColumn(KEY_WORD_UID));
      join = new Join(lo1, lo2, joinConditions, Join.JoinType.INNER);
      operators.add(join);
      lo1 = join;
    }
    return lo1;
  }

  @Deprecated
  // TODO
  private static LogicalOperator getSingleHbaseSegmentScan(String projectId, String userTable, String dateString,
                                                           String propertyName, Object propVal) throws PlanException,
          MemCacheException, TException {
    Map<String, KeyPartParameter> parameterMap = new HashMap<String, KeyPartParameter>(2);
    String originTableName = toUserIndexTableName(projectId);

    Table table;
    try {
      table = getCachedMetaTable(originTableName);
      parameterMap
        .put("propnumber", KeyPartParameter.buildSingleKey(toBytes(propertyString2TinyInt(projectId, propertyName))));
      parameterMap.put("date", KeyPartParameter.buildSingleKey(toBytes(dateString)));
      if (propVal instanceof String) {
        parameterMap.put(KEY_WORD_VALUE, KeyPartParameter.buildSingleKey(toBytes(propVal.toString())));
      } else {
        parameterMap.put(KEY_WORD_VALUE, KeyPartParameter.buildSingleKey(toBytes((Long) propVal)));
      }
    } catch (Exception e) {
      throw new PlanException(e);
    }

    RowkeyRange rowkeyRange;
    try {
      rowkeyRange = Selection.toRowkeyRange(table, parameterMap);
    } catch (TException e) {
      throw new PlanException(e);
    }
    NamedExpression[] projections = new NamedExpression[1];
    projections[0] = new NamedExpression(new FieldReference(KEY_WORD_UID, ExpressionPosition.UNKNOWN),
                                         new FieldReference(KEY_WORD_UID, ExpressionPosition.UNKNOWN));
    Selection selection = new Selection(userTable, rowkeyRange, projections);
    Scan scan;
    try {
      scan = new Scan(SE_HBASE, selection.toSingleJsonOptions(), buildTable(userTable));
    } catch (IOException e) {
      throw new PlanException(e);
    }
    scan.setMemo("Scan@" + userTable + ", Prop=" + propertyName + ", Val=" + propVal);
    return scan;
  }

  public static LogicalOperator getUserScan(String projectId, String groupBy) throws PlanException {
    JSONOptions selection;
    String table = toUserMysqlTableName(projectId, groupBy);
    Map<String, Object> selectionMap = new HashMap<String, Object>(2);
    selectionMap.put("table", table);
    FieldReference uidFR = buildColumn(KEY_WORD_UID);
    NamedExpression[] projections = new NamedExpression[2];
    projections[0] = new NamedExpression(uidFR, uidFR);
    projections[1] = new NamedExpression(buildColumn(KEY_WORD_VAL), buildColumn(groupBy));

    selectionMap.put(KEY_WORD_PROJECTIONS, projections);
    String str;
    ObjectMapper mapper = DEFAULT_DRILL_CONFIG.getMapper();
    try {
      str = mapper.writeValueAsString(selectionMap);
      selection = mapper.readValue(str, JSONOptions.class);
    } catch (Exception e) {
      throw new PlanException(e);
    }
    Scan scan = new Scan(SE_MYSQL, selection, buildTable("user"));
    scan.setMemo("Table=" + table);
    return scan;
  }

  public static LogicalOperator getEventScan(String projectId, String event, String realBeginDate, String realEndDate,
                                             String... additionalProjections) throws PlanException {
    JSONOptions selection;
    XEvent parameterEvent;
    XEventRange eventRange;
    List<XEvent> includes = null;
    XEventOperation operation = XEventOperation.getInstance();
    try {
      parameterEvent = XEvent.buildXEvent(projectId, event);
      if (parameterEvent.isAll()) {
        eventRange = null;
      } else {
        eventRange = operation.getEventRange(projectId, event);
        if (eventRange == null) {
          throw new PlanException("Cannot find any event for project " + projectId);
        }
        includes = parameterEvent.isAmbiguous() ? operation.getEvents(projectId, event, true) : null;
      }
    } catch (Exception e) {
      throw new PlanException(e);
    }

    try {
      selection = getEventSelections(projectId, parameterEvent, eventRange, includes, realBeginDate, realEndDate,
                                     additionalProjections);
    } catch (Exception e) {
      throw new PlanException(e);
    }
    if (selection == null) {
      throw new PlanException("Selection of event table(" + projectId + ") is null.");
    }
    String eventTable = toEventTableName(projectId);
    Scan scan = new Scan(SE_HBASE, selection, buildTable(KEY_WORD_EVENT));
    scan.setMemo(eventTable + "," + realBeginDate + "," + realEndDate + "," + event);
    return scan;
  }

  public static JSONOptions getEventSelections(String projectId, XEvent originalXEvent, XEventRange eventRange,
                                               List<XEvent> includes, String realBeginDate, String realEndDate,
                                               String... additionalProjections) throws IOException, ParseException,
    TException, XEventException, MemCacheException, PlanException {

    // 拆解日期至单天, 保证每个selection都是连续的范围
    List<BeginEndDatePair> dates = DateSplitter.split2Pairs(realBeginDate, realEndDate, Interval.DAY);

    // 从ehcache/meta表获取table信息, 用于组装Rowkey范围, Rowkey的filters
    String genericEventTableName = toEventTableName(KEY_WORD_GENERIC_EVENT_TABLE_NAME);
    Table table;
    try {
      table = getCachedMetaTable(genericEventTableName);
    } catch (MemCacheException e) {
      throw e;
    }

    // 额外的投影运算, 根据外部需要什么投影决定
    NamedExpression[] projections;
    boolean emptyProjections = ArrayUtils.isEmpty(additionalProjections);
    if (emptyProjections) {
      projections = new NamedExpression[2];
    } else {
      projections = new NamedExpression[2 + additionalProjections.length];
    }
    FieldReference fr = buildColumn(KEY_WORD_UID);
    projections[0] = new NamedExpression(fr, fr);
    fr = buildColumn(KEY_WORD_VALUE);
    projections[1] = new NamedExpression(fr, fr);
    if (!emptyProjections) {
      for (int i = 0; i < additionalProjections.length; i++) {
        fr = buildColumn(additionalProjections[i]);
        projections[2 + i] = new NamedExpression(fr, fr);
      }
    }

    // 根据事件层级关系构建event的参数map
    Map<String, KeyPartParameter> parameterMapEvent, parameterMapIncludesEvent;
    List<KeyPartParameter> ranges;
    if (originalXEvent.isAll()) {
      ranges = new ArrayList<KeyPartParameter>(1);
      parameterMapEvent = new HashMap<String, KeyPartParameter>(1);
      byte[] bytes = new byte[]{MIN_EVENT_BYTE};
      ranges.add(KeyPartParameter.buildSingleKey(bytes));
    } else {
      ranges = new ArrayList<KeyPartParameter>(XEventUtils.DEFAULT_EVENT_LEVEL);
      String[] fromS = eventRange.getFrom().getEventArray(), toS = eventRange.getTo().getEventArray();
      byte[] bytes1, bytes2;
      for (int i = 0; i < eventRange.maxLevel(); i++) {
        if (StringUtils.equals(fromS[i], toS[i])) {
          bytes1 = fromS[i] == null ? null : toBytes(fromS[i]);
          ranges.add(KeyPartParameter.buildSingleKey(bytes1));
        } else {
          bytes1 = fromS[i] == null ? null : toBytes(fromS[i]);
          bytes2 = toS[i] == null ? null : toBytes(toS[i]);
          ranges.add(KeyPartParameter.buildRangeKey(bytes1, bytes2));
        }
      }
      parameterMapEvent = new HashMap<String, KeyPartParameter>(6);
    }
    for (int i = 0; i < ranges.size(); i++) {
      parameterMapEvent.put(KEY_WORD_EVENT + i, ranges.get(i));
    }

    //如果需要, 创建rowkey的filter
    List<Map<String, KeyPartParameter>> includeList = null;
    String[] eventArray;
    boolean hasIncludes = CollectionUtils.isNotEmpty(includes);
    Map<String, String> eventLevelMapping = null;
    if (hasIncludes) {
      eventLevelMapping = new HashMap<String, String>(XEventUtils.DEFAULT_EVENT_LEVEL);
      eventArray = originalXEvent.getEventArray();
      for (int i = 0; i < eventArray.length; i++) {
        if (StringUtils.isNotBlank(eventArray[i])) {
          eventLevelMapping.put(KEY_WORD_EVENT + i, eventArray[i]);
        }
      }
      includeList = new ArrayList<Map<String, KeyPartParameter>>(includes.size());
      for (XEvent xEvent : includes) {
        parameterMapIncludesEvent = new HashMap<String, KeyPartParameter>(XEventUtils.DEFAULT_EVENT_LEVEL);
        if (!xEvent.isNormal()) {
          throw new PlanException("Cannot pass ALL or AMBIGUOUS X-Event to rowkey filters");
        }
        eventArray = xEvent.getEventArray();
        for (int i = 0; i < xEvent.maxLevel(); i++) {
          parameterMapIncludesEvent.put(KEY_WORD_EVENT + i, KeyPartParameter.buildSingleKey(toBytes(eventArray[i])));
        }
        includeList.add(parameterMapIncludesEvent);
      }
    }

    Map<String, KeyPartParameter> parameterMap;
    Selection selection;
    RowkeyRange rowkeyRange;
    String eventTableName = toEventTableName(projectId);
    List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>(dates.size());
    String beginDate, d1, d2;
    ScanFilterDescriptor rowkeyFilter;
    for (BeginEndDatePair datePair : dates) {
      parameterMap = new HashMap<String, KeyPartParameter>(XEventUtils.DEFAULT_EVENT_LEVEL + 1);
      beginDate = datePair.getBeginDate();
      d1 = beginDate.replace("-", "");
      if (originalXEvent.isAll()) {
        d2 = dateAdd(beginDate, 1).replace("-", "");
        parameterMap.put("date", KeyPartParameter.buildRangeKey(toBytes(d1), toBytes(d2)));
      } else {
        parameterMap.put("date", KeyPartParameter.buildSingleKey(toBytes(d1)));
      }
      parameterMap.putAll(parameterMapEvent);
      rowkeyRange = Selection.toRowkeyRange(table, parameterMap);
      if (hasIncludes) {
        rowkeyFilter = toRowkeyFilters(table, d1, eventLevelMapping, includeList);
        selection = new Selection(eventTableName, rowkeyRange, projections, rowkeyFilter);
      } else {
        selection = new Selection(eventTableName, rowkeyRange, projections);
      }
      mapList.add(selection.toSelectionMap());
    }
    return DEFAULT_DRILL_CONFIG.getMapper().readValue(DEFAULT_DRILL_CONFIG.getMapper().writeValueAsBytes(mapList),
                                                      JSONOptions.class);
  }

  private static ScanFilterDescriptor toRowkeyFilters(Table table, String dateString,
                                                      Map<String, String> eventLevelMapping,
                                                      List<Map<String, KeyPartParameter>> includeList) throws
    UnsupportedEncodingException, TException {
    LogicalExpression[] les = new LogicalExpression[includeList.size()];
    for (int i = 0; i < includeList.size(); i++) {
      les[i] = toRowkeyFilter(table, dateString, includeList.get(i));
    }
    LogicalExpression[] les2;

    if (MapUtils.isNotEmpty(eventLevelMapping)) {
      les2 = new LogicalExpression[eventLevelMapping.size()];
      LogicalExpression fr, val;
      int i = 0;
      for (Map.Entry<String, String> entry : eventLevelMapping.entrySet()) {
        fr = buildColumn(entry.getKey());
        val = new ValueExpressions.QuotedString(entry.getValue(), ExpressionPosition.UNKNOWN);
        les2[i] = DEFAULT_FUNCTION_REGISTRY.createExpression("==", ExpressionPosition.UNKNOWN, fr, val);
        ++i;
      }
    } else {
      les2 = null;
    }

    return new RowkeyScanFilterDescriptor(ScanFilterType.ROWKEY, les, les2);
  }

  private static LogicalExpression toRowkeyFilter(Table table, String dateString,
                                                  Map<String, KeyPartParameter> include) throws
    UnsupportedEncodingException, TException {
    include.put("date", KeyPartParameter.buildSingleKey(toBytes(dateString)));
    RowkeyRange rowkeyRange = Selection.toRowkeyRange(table, include);
    return new ValueExpressions.QuotedString(rowkeyRange.getStartKey(), ExpressionPosition.UNKNOWN);
  }

  public static PlanProperties buildPlanProperties(String projectId) {
    PlanProperties pp = new PlanProperties();
    pp.generator = new PlanProperties.Generator();
    pp.generator.info = projectId.trim();
    pp.generator.type = "AUTO";
    pp.version = 1;
    pp.type = PlanProperties.PlanType.APACHE_DRILL_LOGICAL;
    return pp;
  }

  public static Store getStore() {
    return new Store("DEFAULT-STORE", null, null);
  }

  public static Table getCachedMetaTable(String tableName) throws TException, MemCacheException {
    Table table;
    XMemCacheManager memCacheManager = MemoryCachedObjects.getInstance().getCacheManager();
    try {
      table = memCacheManager.getCacheElement(MCO_META_TABLE, tableName, Table.class);
    } catch (MemCacheException e) {
      throw e;
    }
    if (table == null) {
      table = Selection.CLIENT.getTable(tableName);
      memCacheManager.putCacheElement(MCO_META_TABLE, tableName, table);
    }
    return table;
  }

}
