package com.xingcloud.qm.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.xingcloud.events.XEventException;
import com.xingcloud.events.XEventOperation;
import com.xingcloud.events.XEventRange;
import com.xingcloud.qm.service.LOPComparator;
import com.xingcloud.qm.service.PlanMerge.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.xingcloud.meta.ByteUtils;
import com.xingcloud.qm.service.PlanSubmission;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.exceptions.DrillRuntimeException;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.graph.AdjacencyList;
import org.apache.drill.common.graph.Edge;
import org.apache.drill.common.graph.GraphAlgos;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.*;
import org.apache.drill.common.util.Selections;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

import static org.apache.drill.common.util.DrillConstants.*;
import static org.apache.drill.common.util.Selections.*;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_ROWKEY_END;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_ROWKEY_START;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 8/30/13
 * Time: 3:07 PM
 * To change this template use File | Settings | File Templates.
 */
public class LogicalPlanUtil {
  public static Logger logger = Logger.getLogger(LogicalPlanUtil.class);

  public static String getTableName(Scan scan) throws Exception {
    try {
      if (scan.getSelection().getRoot() instanceof ArrayNode) {
        return scan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_TABLE).asText();
      } else {
        return scan.getSelection().getRoot().get(SELECTION_KEY_WORD_TABLE).asText();
      }
    } catch (Exception e) {
      throw new Exception(e);
    }
  }

  public static JSONOptions buildJsonOptions(List<Map<String, Object>> mapList, DrillConfig config) throws IOException {
    ObjectMapper mapper = config.getMapper();
    String optionStr = mapper.writeValueAsString(mapList);
    JSONOptions options = mapper.readValue(optionStr, JSONOptions.class);
    return options;
  }

  //todo transfer plan

  public static LogicalPlan transferPlan(LogicalPlan plan, DrillConfig config) throws Exception {
    Collection<SourceOperator> operators = plan.getGraph().getLeaves();
    long t1, t2;
    for (SourceOperator operator : operators) {
      if (operator instanceof Scan) {
        logger.debug("start transfer scan ");
        t1 = System.currentTimeMillis();
        transferScan((Scan) operator, config);
        t2 = System.currentTimeMillis();
        logger.debug("transfer hbase scan using " + (t2 - t1) + " ms");
        logger.debug("transfer scan complete");
      }
    }
    return plan;
  }


  public static void transferScan(Scan scan, DrillConfig config) throws Exception {
    String storageEngine = scan.getStorageEngine();
    String tableName = getTableName(scan);
    if (SE_HBASE.equals(storageEngine)) {
      for (JsonNode selectionNode : scan.getSelection().getRoot()) {
        RowKeyRange range = getRowKeyRangeFromFilter(selectionNode, tableName, config);
        ObjectNode rkRangeNode = new ObjectNode(JsonNodeFactory.instance);
        rkRangeNode.put(SELECTION_KEY_WORD_ROWKEY_START, range.getStartRkStr());
        rkRangeNode.put(SELECTION_KEY_WORD_ROWKEY_END, range.getEndRkStr());
        ((ObjectNode) selectionNode).put(SELECTION_KEY_WORD_ROWKEY, rkRangeNode);
      }
    }
  }

  public static RowKeyRange getRowKeyRangeFromFilter(JsonNode selectionNode, String tableName, DrillConfig config) throws Exception {
    JsonNode filterNode = selectionNode.get(SELECTION_KEY_WORD_FILTER).get(QueryMasterConstant.EXPRESSION);
    return getDeuRkRange(tableName, filterNode, config);
  }

  /**
   * 通过解析filter的表达式获取针对DEU table的start row key 和 end row key
   *
   * @param tableName
   * @param filter
   * @param config
   * @return
   * @throws Exception
   */
  public static RowKeyRange getDeuRkRange(String tableName, JsonNode filter, DrillConfig config) throws Exception {
    Map<String, UnitFunc> fieldFunc = parseFilterExpr(filter, config);
    String eventFilter = getEventFilter(fieldFunc);
    UnitFunc dateUF = fieldFunc.get(QueryMasterConstant.DATE);
    if (dateUF == null) {
      throw new IllegalArgumentException("No date info in expression!");
    }
    String date = dateUF.getValue();
    String srk = null;
    String erk = null;
    if (eventFilter.contains("*")) {
      //需要去mongodb查询得到具体的最小和最大的事件名
      String pID = tableName.replace("deu_", "");
      XEventRange range = XEventOperation.getInstance().getEventRange(pID, eventFilter);
      if (range != null) {
        String eventFrom = range.getFrom().nameRowkeyStyle();
        String eventTo = range.getTo().nameRowkeyStyle();
        srk = date + eventFrom + QueryMasterConstant.XFF + QueryMasterConstant.MIN_UID;
        erk = date + eventTo + QueryMasterConstant.XFF + QueryMasterConstant.MAX_UID;
      } else {
        srk = date + QueryMasterConstant.NA_START_KEY;
        erk = date + QueryMasterConstant.NA_END_KEY;
      }
    } else {
      //已经是具体事件
      srk = date + eventFilter + QueryMasterConstant.XFF + QueryMasterConstant.MIN_UID;
      erk = date + eventFilter + QueryMasterConstant.XFF + QueryMasterConstant.MAX_UID;
    }

    return new RowKeyRange(srk, erk);
  }

  public static String getEventFilter(Map<String, UnitFunc> fieldFunc) {
    StringBuilder eventFilter = new StringBuilder();
    UnitFunc uf = fieldFunc.get(QueryMasterConstant.EVENT0);
    if (uf != null) {
      eventFilter.append(uf.getValue());
    } else {
      eventFilter.append("*");
    }
    eventFilter.append(".");
    uf = fieldFunc.get(QueryMasterConstant.EVENT1);
    if (uf != null) {
      eventFilter.append(uf.getValue());
    } else {
      eventFilter.append("*");
    }
    eventFilter.append(".");
    uf = fieldFunc.get(QueryMasterConstant.EVENT2);
    if (uf != null) {
      eventFilter.append(uf.getValue());
    } else {
      eventFilter.append("*");
    }
    eventFilter.append(".");
    uf = fieldFunc.get(QueryMasterConstant.EVENT3);
    if (uf != null) {
      eventFilter.append(uf.getValue());
    } else {
      eventFilter.append("*");
    }
    eventFilter.append(".");
    uf = fieldFunc.get(QueryMasterConstant.EVENT4);
    if (uf != null) {
      eventFilter.append(uf.getValue());
    } else {
      eventFilter.append("*");
    }
    eventFilter.append(".");
    uf = fieldFunc.get(QueryMasterConstant.EVENT5);
    if (uf != null) {
      eventFilter.append(uf.getValue());
    } else {
      eventFilter.append("*");
    }
    return eventFilter.toString();
  }

  public static Map<String, UnitFunc> parseFilterExpr(JsonNode origExpr, DrillConfig config) throws IOException {
    LogicalExpression func = config.getMapper().readValue(origExpr.traverse(), LogicalExpression.class);
    return parseFunctionCall((FunctionCall) func, config);
  }

  public static Map<String, UnitFunc> parseFunctionCall(FunctionCall func, DrillConfig config) {
    Map<String, UnitFunc> result = new HashMap<>();
    String field = null;
    UnitFunc value ;
    for (LogicalExpression le : func) {
      if (le instanceof FunctionCall) {
        for (Map.Entry<String, UnitFunc> entry : parseFunctionCall(((FunctionCall) le), config).entrySet()) {
          if (result.containsKey(entry.getKey())) {
            LogicalExpression old = result.get(entry.getKey()).getFunc();
            FunctionRegistry registry = new FunctionRegistry(config);
            FunctionCall call = (FunctionCall) registry.createExpression("&&", ExpressionPosition.UNKNOWN, Arrays.asList(old, entry.getValue().getFunc()));
            UnitFunc resultFunc = new UnitFunc(call);
            result.put(field, resultFunc);
          } else
            result.put(entry.getKey(), entry.getValue());
        }
      } else if (le instanceof SchemaPath) {
        field = ((SchemaPath) le).getPath().toString();
        value = new UnitFunc(func);
        if (result.containsKey(field)) {
          LogicalExpression old = result.get(field).getFunc();
          FunctionRegistry registry = new FunctionRegistry(config);
          FunctionCall call = (FunctionCall) registry.createExpression("&&", ExpressionPosition.UNKNOWN, Arrays.asList(old, value.getFunc()));
          value = new UnitFunc(call);
        }
        result.put(field, value);
      }
    }
    return result;
  }

  //todo split scan by rk; contains get base scan---get filter refer to base scan
  //todo -----get project refer to base scan

  public static Scan getBaseScan(RowKeyRange range, List<ScanWithPlan> swps, DrillConfig config) throws IOException {
    List<JsonNode> selectionNodeList = new ArrayList<>();
    for (ScanWithPlan swp : swps) {
      for (JsonNode selectionNode : swp.scan.getSelection().getRoot()) {
        selectionNodeList.add(selectionNode);
      }
    }
    Map<String, Object> selectionMap = new HashMap<>();
    selectionMap.put(SELECTION_KEY_WORD_TABLE, selectionNodeList.get(0).get(SELECTION_KEY_WORD_TABLE).textValue());
    // produce new RowKey range
    Map<String, Object> rkMap = new HashMap<>();
    rkMap.put(SELECTION_KEY_WORD_ROWKEY_START, range.getStartRkStr());
    rkMap.put(SELECTION_KEY_WORD_ROWKEY_END, range.getEndRkStr());
    selectionMap.put(SELECTION_KEY_WORD_ROWKEY, rkMap);

    List<Set<String>> origFilterFieldNames = new ArrayList<>();
    Set<LogicalExpression> origFilterExprSet = new HashSet<>();
    List<Object> projections = new ArrayList<>();
    List<String> projectionExprNames = new ArrayList<>(), projectionRefNames = new ArrayList<>();
    // get projections from orig Projections;
    // get orig filter expressions
    boolean needFilter = true;
    for (JsonNode selectionNode : selectionNodeList) {
      JsonNode ProjectionsNode =
        selectionNode.get(SELECTION_KEY_WORD_PROJECTIONS);
      for (JsonNode projection : ProjectionsNode) {
        String exprName = projection.get(QueryMasterConstant.EXPR).textValue();
        String refName = projection.get(QueryMasterConstant.REF).textValue();
        if (projectionExprNames.contains(exprName) && projectionRefNames.contains(refName)) {
          continue;
        }
        projectionExprNames.add(exprName);
        projectionRefNames.add(refName);
        projections.add(projection);
      }
      JsonNode filterNode = selectionNode.get(SELECTION_KEY_WORD_FILTER);
      if (filterNode == null || filterNode instanceof NullNode) {
        needFilter = false;
        continue;
      }
      origFilterExprSet.add(config.getMapper().readValue((filterNode.get(QueryMasterConstant.EXPRESSION)).traverse(), LogicalExpression.class));
      Map<String, UnitFunc> filterFuncMap = parseFilterExpr(filterNode.get(QueryMasterConstant.EXPRESSION), config);
      origFilterFieldNames.add(filterFuncMap.keySet());
    }

    // get filter expression; it is the or result or orig filter expression set
    Map<String, UnitFunc> filterFields = new HashMap<>();
    if (needFilter) {
      FunctionRegistry registry = new FunctionRegistry(config);
      LogicalExpression filterExpr = origFilterExprSet.size() > 1 ?
        registry.createExpression(QueryMasterConstant.OR, ExpressionPosition.UNKNOWN, new ArrayList<>(origFilterExprSet)) :
        new ArrayList<>(origFilterExprSet).get(0);
      filterFields = getCommonFields((FunctionCall) filterExpr, config);
      Map<String, Object> filter = new HashMap<>();
      filter.put(QueryMasterConstant.EXPRESSION, filterExpr);
      selectionMap.put(SELECTION_KEY_WORD_FILTER, filter);
    }

    // get additional projections from filter expression
    for (Set<String> fieldNames : origFilterFieldNames) {
      for (String fieldName : fieldNames) {
        if (!filterFields.containsKey(fieldName)) {
          if (projectionExprNames.contains(fieldName) && projectionRefNames.contains(fieldName)) {
            continue;
          }
          Map<String, Object> projectionMap = new HashMap<>();
          projections.add(projectionMap);
          projectionMap.put(QueryMasterConstant.REF, fieldName);
          projectionMap.put(QueryMasterConstant.EXPR, fieldName);
          projectionExprNames.add(fieldName);
          projectionRefNames.add(fieldName);
        }
      }
    }
    selectionMap.put(SELECTION_KEY_WORD_PROJECTIONS, projections);
    List<Map<String, Object>> resultSelectionMapList = new ArrayList<>();
    resultSelectionMapList.add(selectionMap);
    // produce base scan
    JSONOptions selection = LogicalPlanUtil.buildJsonOptions(resultSelectionMapList, config);
    FieldReference ref = swps.get(0).scan.getOutputReference();
    String storageEngine = swps.get(0).scan.getStorageEngine();
    Scan scan = new Scan(storageEngine, selection, ref);
    String tableName = swps.get(0).tableName;
    scan.setMemo(tableName + ":" + Bytes.toStringBinary(range.getStartRowKey()).substring(0, 8) + ", "
      + Bytes.toStringBinary(range.getEndRowKey()).substring(0, 8));
    return scan;
  }

  public static Map<String, UnitFunc> getCommonFields(FunctionCall baseFilterExpr, DrillConfig config) {
    List<Map<String, UnitFunc>> funcMaps = getFuncMaps(baseFilterExpr, config);
    Map<String, UnitFunc> refFuncMap = funcMaps.get(0);
    List<String> excludingFieldNames = new ArrayList<>();
    for (Map.Entry<String, UnitFunc> entry : refFuncMap.entrySet()) {
      String fieldName = entry.getKey();
      UnitFunc value = entry.getValue();
      for (int i = 1; i < funcMaps.size(); i++) {
        if (!funcMaps.get(i).containsKey(fieldName) || !funcMaps.get(i).get(fieldName).equals(value)) {
          excludingFieldNames.add(fieldName);
          break;
        }
      }
    }
    for (String fieldName : excludingFieldNames) {
      refFuncMap.remove(fieldName);
    }
    return refFuncMap;
  }

  public static List<Map<String, UnitFunc>> getFuncMaps(FunctionCall filterExpr, DrillConfig config) {
    List<Map<String, UnitFunc>> funcMaps = new ArrayList<>();
    if (!filterExpr.getDefinition().getName().contains("or"))
      funcMaps.add(parseFunctionCall(filterExpr, config));
    else {
      for (LogicalExpression le : filterExpr) {
        if (((FunctionCall) le).getDefinition().getName().contains("or"))
          funcMaps.addAll(getFuncMaps(((FunctionCall) le), config));
        else
          funcMaps.add(parseFunctionCall(((FunctionCall) le), config));
      }
    }
    return funcMaps;
  }

  public static Filter getFilter(Scan baseScan, Scan scan, DrillConfig config) throws IOException {
    List<LogicalExpression> filterEntry = getFilterEntry(baseScan, scan, config);
    LogicalExpression filterExpr;
    Filter filter;
    if (filterEntry == null) {
      return null;
    }
    if (filterEntry.size() > 1) {
      FunctionRegistry registry = new FunctionRegistry(config);
      filterExpr = registry.createExpression("&&", ExpressionPosition.UNKNOWN, filterEntry);
      filter = new Filter(filterExpr);
    } else {
      filterExpr = filterEntry.get(0);
      filter = new Filter(filterExpr);
    }
    return filter;
  }

  public static List<LogicalExpression> getFilterEntry(Scan baseScan, Scan scan, DrillConfig config) throws IOException {
    JsonNode baseFilterExprNode = baseScan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_FILTER).get("expression");
    FunctionCall baseFilterExpr = (FunctionCall) config.getMapper().readValue(baseFilterExprNode.traverse(), LogicalExpression.class);
    JsonNode fitlerExpr = scan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_FILTER).get(SELECTION_KEY_WORD_FILTER_EXPRESSION);
    Map<String, UnitFunc> filterFuncMap = parseFilterExpr(fitlerExpr, config);
    String eventFilter = getEventFilter(filterFuncMap);
    if(eventFilter.endsWith("."))
      eventFilter=eventFilter.substring(0,eventFilter.length()-1);
    while(eventFilter.endsWith(".*"))
      eventFilter=eventFilter.substring(0,eventFilter.length()-2);
    if(!eventFilter.contains(".*."))
        return null;
    Map<String, UnitFunc> baseFilterFields = getCommonFields(baseFilterExpr, config);
    if (baseFilterFields.size() >= filterFuncMap.size()) return null;
    List<LogicalExpression> exprs = new ArrayList<>();
    for (String fieldName : baseFilterFields.keySet()) {
      if (filterFuncMap.containsKey(fieldName))
        filterFuncMap.remove(fieldName);
    }
    for (Map.Entry<String, UnitFunc> entry : filterFuncMap.entrySet()) {
      LogicalExpression le = entry.getValue().getFunc();
      exprs.add(le);
    }
    return exprs;
  }

  public static Project getProject(Scan scan, DrillConfig config) throws IOException {
    List<NamedExpression> nes = getProjectionEntry(scan, config);
    NamedExpression[] namedExpressions = nes.toArray(new NamedExpression[nes.size()]);
    Project project = new Project(namedExpressions);
    return project;
  }

  public static List<NamedExpression> getProjectionEntry(Scan scan, DrillConfig config) {
    List<NamedExpression> nes = new ArrayList<>();
    try {
      if (scan.getSelection().getRoot() instanceof ArrayNode) {
        for (JsonNode selectionNode : scan.getSelection().getRoot()) {
          JsonNode projections = selectionNode.get(SELECTION_KEY_WORD_PROJECTIONS);
          for (JsonNode projection : projections) {
            FieldReference ref = new FieldReference(projection.get("ref").textValue(), null);
            LogicalExpression le = new SchemaPath(projection.get("expr").textValue(), null);
            NamedExpression ne = new NamedExpression(le, ref);
            nes.add(ne);
          }
        }
      } else if (scan.getSelection().getRoot() instanceof ObjectNode) {
        JsonNode projections = scan.getSelection().getRoot().get(SELECTION_KEY_WORD_PROJECTIONS);
        for (JsonNode projection : projections) {
          FieldReference ref = new FieldReference(projection.get("ref").textValue(), null);
          LogicalExpression le = new SchemaPath(projection.get("expr").textValue(), null);
          NamedExpression ne = new NamedExpression(le, ref);
          nes.add(ne);
        }
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
    return nes;
  }

  public static LogicalPlanUtil.RowKeyRange getRowKeyRange(Scan scan) {
    if (!scan.getStorageEngine().equals(QueryMasterConstant.HBASE)) {
      throw new IllegalArgumentException(scan.getStorageEngine() + " has no row key range info!");
    }
    JsonNode selection = scan.getSelection().getRoot().get(0);
    JsonNode rowKey = selection.get(Selections.SELECTION_KEY_WORD_ROWKEY);
    String startRowKey = rowKey.get(Selections.SELECTION_KEY_WORD_ROWKEY_START).textValue();
    String endRowKey = rowKey.get(Selections.SELECTION_KEY_WORD_ROWKEY_END).textValue();
    return new LogicalPlanUtil.RowKeyRange(startRowKey, endRowKey);
  }

  public static boolean isRkRangeInScan(byte[] rkPoint, ScanWithPlan swp) {
    RowKeyRange scanRange = swp.range;
    byte[] scanSrk = scanRange.getStartRowKey(), scanEnk = scanRange.getEndRowKey();
    if (Bytes.compareTo(scanEnk, rkPoint) >= 0 && Bytes.compareTo(scanSrk, rkPoint) < 0) {
      return true;
    }
    return false;
  }

  //todo merge plans;catogery is merge same scan
  public static List<LogicalOperator> getParents(LogicalOperator scan, LogicalPlan plan) {
    AdjacencyList<LogicalOperator> child2Parents = plan.getGraph().getAdjList().getReversedList();
    Collection<AdjacencyList<LogicalOperator>.Node> leaves = child2Parents.getInternalRootNodes();
    AdjacencyList<LogicalOperator>.Node child = null;
    List<LogicalOperator> parents = new ArrayList<>();
    for (AdjacencyList<LogicalOperator>.Node leaf : leaves) {
      if (leaf.getNodeValue().equals(scan)) {
        child = leaf;
        break;
      }
    }
    List<Edge<AdjacencyList<LogicalOperator>.Node>> parentEdges = child2Parents.getAdjacent(child);
    for (Edge<AdjacencyList<LogicalOperator>.Node> edge : parentEdges) {
      parents.add(edge.getTo().getNodeValue());
    }
    return parents;
  }


  public static void substituteInParent(LogicalOperator source, LogicalOperator target, LogicalOperator parent) {
    if (parent instanceof SingleInputOperator) {
      ((SingleInputOperator) parent).setInput(target);
    } else if (parent instanceof Join) {
      Join join = (Join) parent;
      if (join.getLeft() == source) {
        join.setLeft(target);
      } else if (join.getRight() == (source)) {
        join.setRight(target);
      }
    } else if (parent instanceof Union) {
      Union union = (Union) parent;
      LogicalOperator[] inputs = union.getInputs();
      for (int j = 0; j < inputs.length; j++) {
        LogicalOperator input = inputs[j];
        if (input == (source)) {
          inputs[j] = target;
          break;
        }
      }
    } else {
      throw new IllegalArgumentException("operator not supported!" + parent);
    }
  }


  public static Mergeability<LogicalOperator> equals(LogicalOperator op1, LogicalOperator op2) {
    if (LOPComparator.equals(op1, op2)) {
      return new Mergeability<>(MergeType.same, op1, op2);
    }
    return null;
  }

  /**
   * 判断两个plan是否合并： 如果两个plan有相同的scan，那就合并。 如果两个plan，其中一个的scan范围包含另外一个，那就合并。 TODO 现在只合并一模一样的scan。需要更多的merge的策略和相应的数据
   *
   * @param scan1
   * @param scan2
   * @return
   */
  public static Mergeability<Scan> mergeable(Scan scan1, Scan scan2) {
    return LOPComparator.equals(scan1, scan2) ? new Mergeability<Scan>(MergeType.same, scan1, scan2) : null;
  }

  public static Mergeability<Scan> equals(Scan scan1, Scan scan2) throws Exception {
    if (getTableName(scan1).equals(getTableName(scan2)) && scan1.getSelection().equals(scan2.getSelection())) {
      return new Mergeability<>(MergeType.same, scan1, scan2);
    }
    return null;
  }

  public static class Mergeability<T extends LogicalOperator> {

    public MergeType mergeType;
    public T from;
    public T to;

    Mergeability() {
    }

    public Mergeability(MergeType mergeType, T from, T to) {
      this.mergeType = mergeType;
      this.from = from;
      this.to = to;
    }

  }

  public static class UnitFunc {
    private String field;
    private String op;
    private String value;
    private FunctionCall func;

    public UnitFunc() {

    }

    public UnitFunc(FunctionCall func) {
      this.func = func;
      for (LogicalExpression le : func) {
        if (le instanceof SchemaPath) {
          field = ((SchemaPath) le).getPath().toString();
        } else if (le instanceof ValueExpressions.QuotedString) {
          value = ((ValueExpressions.QuotedString) le).value;
        }
      }
      op = func.getDefinition().getName();
    }

    public UnitFunc(String field, String op, String value) {
      this.field = field;
      this.op = op;
      this.value = value;
    }

    public String getField() {
      return field;
    }

    public String getOp() {
      return op;
    }

    public String getValue() {
      return value;
    }

    public FunctionCall getFunc() {
      return func;
    }

    public boolean equals(Object o) {
      if (!(o instanceof UnitFunc))
        return false;
      if (func.equals(((UnitFunc) o).getFunc()))
        return true;
      return false;
    }
  }

  public static enum MergeType {
    same, belongsto, sametable
  }

  public static class RowKeyRange {
    byte[] startRowKey, endRowKey;
    String startRkStr, endRkStr;

    public RowKeyRange(String srk, String enk) {
      this.startRowKey = Bytes.toBytesBinary(srk);
      this.endRowKey = Bytes.toBytesBinary(enk);
      this.startRkStr = srk;
      this.endRkStr = enk;
    }

    public RowKeyRange(byte[] srk, byte[] enk) {
      this.startRowKey = srk;
      this.endRowKey = enk;
      this.startRkStr = Bytes.toStringBinary(srk);
      this.endRkStr = Bytes.toStringBinary(enk);
    }

    public byte[] getStartRowKey() {
      return startRowKey;
    }

    public byte[] getEndRowKey() {
      return endRowKey;
    }

    public String getStartRkStr() {
      return startRkStr;
    }

    public String getEndRkStr() {
      return endRkStr;
    }

    public String toString() {
      return "srk: " + ByteUtils.toStringBinary(startRowKey) + "\n"
        + "enk: " + ByteUtils.toStringBinary(endRowKey);
    }
  }

  public static class ScanRkCompartor implements Comparator<ScanWithPlan> {

    @Override
    public int compare(ScanWithPlan o1, ScanWithPlan o2) {

      RowKeyRange range1 = getRowKeyRange(o1.scan);
      RowKeyRange range2 = getRowKeyRange(o2.scan);
      return Bytes.compareTo(range1.getStartRowKey(), range2.getStartRowKey());
    }
  }

  public static Pair<Long, Long> getLocalSEUidOfBucket(int startBucketPos, int offsetBucketLen) {
    long endBucket = 0;
    if (startBucketPos + offsetBucketLen >= 255) {
      endBucket = (1l << 40) - 1l;
    } else {
      endBucket = startBucketPos + offsetBucketLen;
      endBucket = endBucket << 32;
    }
    long startBucket = (long)startBucketPos << 32;
    return new Pair(startBucket, endBucket);
  }

  public static Pair<byte[], byte[]> changeTo5Bytes(Pair<Long, Long> uidRange) {
    Pair<byte[], byte[]> bytesPair = new Pair<>();
    byte[] suid = Bytes.toBytes(uidRange.getFirst());
    byte[] euid = Bytes.toBytes(uidRange.getSecond());
    byte[] suid5 = Arrays.copyOfRange(suid, 3, 8);
    byte[] euid5 = Arrays.copyOfRange(euid, 3, 8);
    return new Pair<byte[], byte[]>(suid5, euid5);
  }


  /**
   * 增加uid range信息到scan(storage engine为mysql和hbase)
   * @param mergedPlan
   * @param startBucketPos
   * @param offsetBucketLen
   * @throws IOException
   */
  public static void addUidRangeInfo(LogicalPlan mergedPlan, int startBucketPos, int offsetBucketLen) throws IOException {
    Pair<Long, Long> uidRange = getLocalSEUidOfBucket(startBucketPos, offsetBucketLen);
    Pair<byte[], byte[]> uidRange5Bytes = changeTo5Bytes(uidRange);
    Collection<SourceOperator> leaves = mergedPlan.getGraph().getLeaves();
    for (SourceOperator leaf : leaves) {
      if (leaf instanceof Scan) {
        logger.debug("Add uid range info into " + leaf);
        JSONOptions selections = ((Scan) leaf).getSelection();
        ArrayNode selectionNodes = (ArrayNode)selections.getRoot();
        if (((Scan) leaf).getStorageEngine().equals(QueryMasterConstant.STORAGE_ENGINE.hbase.name())) {
          for (JsonNode selection : selectionNodes) {
            JsonNode rkRange = selection.get(SELECTION_KEY_WORD_ROWKEY);
            String startRK = rkRange.get(SELECTION_KEY_WORD_ROWKEY_START).textValue();
            String endRK = rkRange.get(SELECTION_KEY_WORD_ROWKEY_END).textValue();
            byte[] srk = Bytes.toBytesBinary(startRK);
            changeUidBytes(srk, uidRange5Bytes.getFirst(), uidRange5Bytes.getSecond());
            byte[] erk = Bytes.toBytesBinary(endRK);
            changeUidBytes(erk, uidRange5Bytes.getFirst(), uidRange5Bytes.getSecond());
            ((ObjectNode)rkRange).put(SELECTION_KEY_WORD_ROWKEY_START, Bytes.toStringBinary(srk));
            ((ObjectNode)rkRange).put(SELECTION_KEY_WORD_ROWKEY_END, Bytes.toStringBinary(erk));

            //增加一个字段Tail range，代表需扫描的起始和结束uid
            JsonNode tailRange = new ObjectNode(JsonNodeFactory.instance);
            ((ObjectNode)tailRange).put(SELECTION_KEY_ROWKEY_TAIL_START, Bytes.toStringBinary(uidRange5Bytes.getFirst()));
            ((ObjectNode)tailRange).put(SELECTION_KEY_ROWKEY_TAIL_END, Bytes.toStringBinary(uidRange5Bytes.getSecond()));
            ((ObjectNode)selection).put(SELECTION_KEY_ROWKEY_TAIL_RANGE, tailRange);
          }
        } else if (((Scan) leaf).getStorageEngine().equals(QueryMasterConstant.STORAGE_ENGINE.mysql.name())) {
          for (JsonNode selection : selectionNodes) {
            //Mysql把uid range信息加入到filter里（expression符合drill的logical expression规则）
            JsonNode filter = selection.get(SELECTION_KEY_WORD_FILTER);
            String uidRangeStr = "( (uid) >= (" + uidRange.getFirst() +
                    ") ) && ( (uid) < (" + uidRange.getSecond() + ") )";
            if (filter != null) {
              String expr = filter.get(SELECTION_KEY_WORD_FILTER_EXPRESSION).textValue();
              expr = expr + " && " + uidRangeStr;
              ((ObjectNode)filter).put(SELECTION_KEY_WORD_FILTER_EXPRESSION, expr);
            } else {
              filter = new ObjectNode(JsonNodeFactory.instance);
              ((ObjectNode)filter).put(SELECTION_KEY_WORD_FILTER_EXPRESSION, uidRangeStr);
              ((ObjectNode)selection).put(SELECTION_KEY_WORD_FILTER, filter);
            }
          }
        }
      }
    }
  }

  /**
   * 根据Entry拆分UnionedScan，使drill-bit可以并发执行
   * @param plan
   * @param splitNum
   */
  public static LogicalPlan splitUnionedScan(LogicalPlan plan, int splitNum) throws IOException {
    logger.info("Begin to split merged plan's UnionedScan, split number: " + splitNum);
    assert splitNum > 0;
    if (splitNum == 1) {
      return plan;
    }
    ObjectMapper mapper = DrillConfig.create().getMapper();
    Collection<SourceOperator> leaves = plan.getGraph().getLeaves();
    List<LogicalOperator> originScan = new ArrayList<>();
    List<LogicalOperator> operators = plan.getSortedOperators();

    for (SourceOperator scan : leaves) {
      if (scan instanceof UnionedScan) {
        JSONOptions selections = ((UnionedScan) scan).getSelection();
        ArrayNode selectionNodes = (ArrayNode)selections.getRoot();
        int totalEntryNum = selectionNodes.size();
        int each = (int) Math.ceil((double)totalEntryNum / (double)splitNum);
        if (each == 0) {
          logger.info("Entry number(" + totalEntryNum + ") is less than " + splitNum);
          continue;
        }
        originScan.add(scan);
        List<ArrayNode> splitSelections = new ArrayList<>();
        int i = 0;
        ArrayNode eachSelections = new ArrayNode(mapper.getNodeFactory());
        //对UnionedScan中的entry重新分组
        for (JsonNode selection : selectionNodes) {
          if (i > each-1) {
            i = 0;
            splitSelections.add(eachSelections);
            eachSelections = new ArrayNode(mapper.getNodeFactory());
          }
          eachSelections.add(selection);
          i++;
        }
        if (eachSelections.size() != 0) {
          splitSelections.add(eachSelections);
        }

        //根据重新分组的entry重新构造UnionedScan
        List<UnionedScan> unionedScans = new ArrayList<>();
        for (ArrayNode selectionsGroup : splitSelections) {
          String selectionStr = mapper.writeValueAsString(selectionsGroup);
          JSONOptions newSelections = mapper.readValue(selectionStr, JSONOptions.class);
          UnionedScan unionedScan = new UnionedScan(((UnionedScan) scan).getStorageEngine(),
                  newSelections, ((UnionedScan) scan).getOutputReference());
          unionedScans.add(unionedScan);
          operators.add(unionedScan);
        }
        //更新UnionedScanSplit与UnionedScan的关系
        List<LogicalOperator> parents = scan.getAllSubscribers(); //op: unioned-scan-split
        for (LogicalOperator parent : parents) {
          UnionedScanSplit unionedScanSplit = (UnionedScanSplit) parent;
          int[] originEntries = unionedScanSplit.getEntries();
          //每个UnionedScanSplit必须只唯一对应一个entry
          assert originEntries.length == 1;
          //与原始UnionedScan撤销关系
          scan.unregisterSubscriber(unionedScanSplit);
          //注册新的UnionedScan
          int entryID = originEntries[0];
          int inputPos = (entryID) / each;
          int entryPos = (entryID) % each;
          unionedScanSplit.setInput(unionedScans.get(inputPos));
          int[] newEntires = {entryPos};
          unionedScanSplit.setEntries(newEntires);
        }
      }
    }
    operators.removeAll(originScan);
    LogicalPlan newPlan = new LogicalPlan(plan.getProperties(), plan.getStorageEngines(), operators);
    return newPlan;
  }

  public static LogicalPlan copyPlan(LogicalPlan plan) {
    LogicalPlan copy = null;
    DrillConfig c = DrillConfig.create();
    String planJson = null;
    try {
      planJson = plan.toJsonString(c);
      copy = c.getMapper().readValue(planJson, LogicalPlan.class);
    } catch (JsonProcessingException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    } catch (IOException e) {
      e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
    }
    return copy;
  }

  public static void changeUidBytes(byte[] rk, byte[] suid, byte[] euid) {
    if (Bytes.compareTo(rk, rk.length-5, 5, QueryMasterConstant.MIN_UID_BYTES, 0, 5) == 0) {
      System.arraycopy(suid, 0, rk, rk.length-5, 5);
    } else {
      System.arraycopy(euid, 0, rk, rk.length-5, 5);
    }
  }

  public static List<Integer> generateSapmleList(String pID, Set<String> eventPatterns) throws XEventException {
    long st = System.nanoTime();
    long oneBucketCount = 2400000;
    Map<String, Long> avgMap = XEventOperation.getInstance().getAvgCountOfEachEvent(pID, eventPatterns);
    List<Integer> initSampleList = new ArrayList<>();
    Set<Integer> sampleSet = new HashSet<>();
    StringBuilder summary = new StringBuilder("------ Predict scan bucket summary[" + pID + "]:\n");
    for (Map.Entry<String, Long> entry : avgMap.entrySet()) {
      String event = entry.getKey();
      long avg = entry.getValue();
      int bucketNum = (int)(avg / oneBucketCount);
      int scanBucket = 0;
      if (bucketNum == 0) {
        //不足一个桶，全扫
        scanBucket = 256;
      } else if (bucketNum > 256) {
        //大于256个桶，先扫1个桶
        scanBucket = 1;
      } else {
        //扫oneBucketCount所达到的桶
        scanBucket = 256/bucketNum;
      }
      sampleSet.add(scanBucket);
      summary.append(event).append(": [AVG: " + avg + "]\t[Bucket: " + scanBucket + "]").append("\n");
    }
    //防止预测的扫描桶数都不满足人数阈值，增加全扫桶数
    sampleSet.add(256);
    //获得增量采样序列
    for (int initBucket : sampleSet) {
      initSampleList.add(initBucket);
    }
    Collections.sort(initSampleList);
    List<Integer> incrementSample = new ArrayList<>();
    summary.append("Scan bucket array:\t");

    incrementSample.add(initSampleList.get(0));
    summary.append(initSampleList.get(0)).append(",");
    for (int i=1; i<initSampleList.size(); i++) {
      int incrementBucket = initSampleList.get(i)-initSampleList.get(i-1);
      incrementSample.add(incrementBucket);
      summary.append(incrementBucket).append(",");
    }
    summary.append("\n").append("Taken: " + (System.nanoTime()-st)/1.0e9 + " sec");
    logger.info(summary.toString());
    return incrementSample;
  }

  public static Set<String> getEventPatterns(PlanSubmission submission) {
    Set<String> eventPatterns = new HashSet<>();
    for (String qid : submission.queryIdToPlan.keySet()) {
      String[] fields = qid.split(",");
      if (fields.length < 5) {
        throw new DrillRuntimeException("Cache key is invalid!. + " + qid);
      }
      eventPatterns.add(fields[4]);
    }
    return eventPatterns;
  }

}
