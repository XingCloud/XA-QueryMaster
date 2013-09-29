package com.xingcloud.qm.utils;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.xingcloud.events.XEventOperation;
import com.xingcloud.events.XEventRange;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.qm.service.LOPComparator;
import com.xingcloud.qm.service.PlanMerge.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.xingcloud.meta.ByteUtils;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.expression.*;
import org.apache.drill.common.graph.AdjacencyList;
import org.apache.drill.common.graph.Edge;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.data.*;
import org.apache.drill.common.util.Selections;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;

import static org.apache.drill.common.util.DrillConstants.SE_HBASE;
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

    public static JSONOptions buildJsonOptions(List<Map<String, Object>> mapList, DrillConfig config) throws IOException {
        ObjectMapper mapper = config.getMapper();
        String optionStr = mapper.writeValueAsString(mapList);
        JSONOptions options = mapper.readValue(optionStr, JSONOptions.class);
        return options;
    }

    public static Scan getBaseScan(RowKeyRange range, List<ScanWithPlan> swps, DrillConfig config) throws IOException {
        List<JsonNode> selectionNodeList = new ArrayList<>();
        for (ScanWithPlan swp : swps) {
            for (JsonNode selectionNode : swp.scan.getSelection().getRoot()) {
                selectionNodeList.add(selectionNode);
            }
        }
        Map<String, Object> selectionMap = new HashMap<>();
        selectionMap.put(SELECTION_KEY_WORD_TABLE, selectionNodeList.get(0).get(SELECTION_KEY_WORD_TABLE).textValue());
        //RowKey range
        Map<String, Object> rkMap = new HashMap<>();
        rkMap.put(SELECTION_KEY_WORD_ROWKEY_START, range.getStartRkStr());
        rkMap.put(SELECTION_KEY_WORD_ROWKEY_END, range.getEndRkStr());
        selectionMap.put(SELECTION_KEY_WORD_ROWKEY, rkMap);

        List<Set<String>> filterFields = new ArrayList<>();
        List<LogicalExpression> baseFilterExprs;
        Set<LogicalExpression> baseFilterExprSet = new HashSet<>();
        List<String> baseFilterFields = new ArrayList<>();
        List<Object> projections = new ArrayList<>();
        List<String> projectionExprNames = new ArrayList<>(), projectionRefNames = new ArrayList<>();

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
          baseFilterExprSet.add(config.getMapper().readValue((filterNode.get(QueryMasterConstant.EXPRESSION)).traverse(), LogicalExpression.class));
          Map<String, UnitFunc> filterFuncMap = parseFilterExpr(filterNode.get(QueryMasterConstant.EXPRESSION), config);
          filterFields.add(filterFuncMap.keySet());
        }

        List<String> filterFieldNames=new ArrayList<>();
        if (needFilter) {
          FunctionRegistry registry = new FunctionRegistry(config);
          baseFilterExprs = new ArrayList<>(baseFilterExprSet);
          LogicalExpression filterExpr = baseFilterExprs.size() > 1 ? registry.createExpression(QueryMasterConstant.OR,
                  ExpressionPosition.UNKNOWN, baseFilterExprs) : baseFilterExprs.get(0);
          filterFieldNames = getCommonFields((FunctionCall)filterExpr, config);
          Map<String, Object> filter = new HashMap<>();
          filter.put(QueryMasterConstant.EXPRESSION, filterExpr);
          selectionMap.put(SELECTION_KEY_WORD_FILTER, filter);

          Set<String> filterField = filterFields.get(0);
          for (int i=1; i<filterFields.size(); i++) {
            Set<String> filterFieldTmp = filterFields.get(i);
            for (String field : filterFieldNames) {
              if (!filterFieldTmp.contains(field)) {
                  filterField.remove(field);
              }
            }
          }
          baseFilterFields.addAll(filterField);
        }
        for (Set<String> fields : filterFields) {
          for (String field : fields) {
            if (!filterFieldNames.contains(field)) {
              if (projectionExprNames.contains(field) && projectionRefNames.contains(field)) {
                continue;
              }
              Map<String, Object> projectionMap = new HashMap<>();
              projections.add(projectionMap);
              projectionMap.put(QueryMasterConstant.REF, field);
              projectionMap.put(QueryMasterConstant.EXPR, field);
              projectionExprNames.add(field);
              projectionRefNames.add(field);
            }
          }
        }
        selectionMap.put(SELECTION_KEY_WORD_PROJECTIONS, projections);
        List<Map<String, Object>> resultSelectionMapList = new ArrayList<>();
        resultSelectionMapList.add(selectionMap);

        JSONOptions selection = LogicalPlanUtil.buildJsonOptions(resultSelectionMapList, config);
        FieldReference ref = swps.get(0).scan.getOutputReference();
        String storageEngine = swps.get(0).scan.getStorageEngine();
        Scan scan = new Scan(storageEngine, selection, ref);
        String tableName = swps.get(0).tableName;
        scan.setMemo(tableName + ":" + Bytes.toStringBinary(range.getStartRowKey()).substring(0, 8) + ", "
                + Bytes.toStringBinary(range.getEndRowKey()).substring(0, 8));
        return scan;
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
        String rkPattern = getRkPattern(scan, config);
        if (!rkPattern.contains(".*.")) return null;
        JsonNode baseFilterExprNode = baseScan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_FILTER).get("expression");
        FunctionCall baseFilterExpr=(FunctionCall)config.getMapper().readValue(baseFilterExprNode.traverse(),LogicalExpression.class);
        JsonNode fitlerExpr = scan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_FILTER).get("expression");
        Map<String, UnitFunc> filterFuncMap = parseFilterExpr(fitlerExpr, config);
        List<String> baseFilterFields = getCommonFields(baseFilterExpr, config);
        if (baseFilterFields.size() >= filterFuncMap.size()) return null;
        List<LogicalExpression> exprs = new ArrayList<>();
        for (String field : baseFilterFields) {
            if (filterFuncMap.containsKey(field))
                filterFuncMap.remove(field);
        }
        for (Map.Entry<String, UnitFunc> entry : filterFuncMap.entrySet()) {
            LogicalExpression le = entry.getValue().getFunc();
            exprs.add(le);
        }
        return exprs;
    }

    public static String getRkPattern(Scan scan, DrillConfig config) throws IOException {
        JsonNode filterNode = scan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_FILTER).get("expression");
        String tableName = scan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_TABLE).textValue();
        try {
            List<KeyPart> kps = MetaUtil.getInstance().getTableRkKps(tableName);
            String rkPattern = "";
            Map<String, UnitFunc> fieldFunc = parseFilterExpr(filterNode, config);
            List<KeyPart> workKps = kps;
            Deque<KeyPart> toWorkKps = new ArrayDeque<>(workKps);
            loop:
            while (workKps.size() > 0) {
                for (KeyPart kp : workKps) {
                    if (kp.getType() == KeyPart.Type.field) {
                        String value;
                        UnitFunc unitFunc = fieldFunc.get(kp.getField().getName());
                        if (unitFunc != null)
                            value = unitFunc.getValue();
                        else
                            value = "*";
                        rkPattern += value;
                        toWorkKps.removeFirst();
                        fieldFunc.remove(kp.getField().getName());
                        if (fieldFunc.size() == 0)
                            break loop;
                    } else if (kp.getType() == KeyPart.Type.constant) {
                        rkPattern += kp.getConstant();
                        toWorkKps.removeFirst();
                    } else {
                        toWorkKps.removeFirst();
                        for (int i = kp.getOptionalGroup().size() - 1; i >= 0; i--) {
                            toWorkKps.addFirst(kp.getOptionalGroup().get(i));
                        }
                        break;
                    }
                }
                workKps = Arrays.asList(toWorkKps.toArray(new KeyPart[toWorkKps.size()]));
            }
            if (rkPattern.endsWith("."))
                rkPattern = rkPattern.substring(0, rkPattern.length() - 1);
            while (rkPattern.endsWith(".*"))
                rkPattern = rkPattern.substring(0, rkPattern.length() - 2);

            return rkPattern;
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e.getMessage());
        }
    }

    public static List<String> getCommonFields(FunctionCall baseFilterExpr,DrillConfig config){
        List<Map<String,UnitFunc>> funcMaps=getFuncMaps(baseFilterExpr,config);
        Map<String,UnitFunc> refFuncMap=funcMaps.get(0);
        List<String> excludingFields=new ArrayList<>();
        for(Map.Entry<String,UnitFunc> entry : refFuncMap.entrySet()){
            String field=entry.getKey();
            UnitFunc value=entry.getValue();
            for(int i=1;i<funcMaps.size();i++){
                if(!funcMaps.get(i).containsKey(field)|| !funcMaps.get(i).get(field).equals(value))
                {
                    excludingFields.add(field);
                    break;
                }
            }
        }
        for(String field :excludingFields){
            refFuncMap.remove(field);
        }
        return new ArrayList<>(refFuncMap.keySet());
    }

    public static List<Map<String,UnitFunc>> getFuncMaps(FunctionCall filterExpr,DrillConfig config){
        List<Map<String, UnitFunc>> funcMaps = new ArrayList<>();
        if (!filterExpr.getDefinition().getName().contains("or"))
            funcMaps.add(parseFunctionCall(filterExpr, config));
        else
            for (LogicalExpression le : filterExpr) {
                if (((FunctionCall) le).getDefinition().getName().contains("or"))
                    funcMaps.addAll(getFuncMaps(((FunctionCall) le), config));
                else
                    funcMaps.add(parseFunctionCall(((FunctionCall) le), config));
            }
        return funcMaps;
    }

    public List<String> getFields(JsonNode filterNode, DrillConfig config) throws IOException {
        try {
            return getFields((LogicalExpression) config.getMapper().readValues(filterNode.get("expression").traverse(), LogicalExpression.class), config);
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }

    public List<String> getFields(LogicalExpression filterExpr, DrillConfig config) {
        Set<String> fields = new HashSet<>();
        if (!(filterExpr instanceof FunctionCall))
            return null;
        for (LogicalExpression childExpr : (FunctionCall) filterExpr) {
            if (childExpr instanceof SchemaPath) {
                fields.add(((SchemaPath) childExpr).getPath().toString());
            } else if (childExpr instanceof FunctionCall)
                fields.addAll(getFields(childExpr, config));
        }
        return new ArrayList<>(fields);
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
            for (JsonNode selectionNode : scan.getSelection().getRoot()) {
                JsonNode projections = selectionNode.get(SELECTION_KEY_WORD_PROJECTIONS);
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


    public boolean isRowKeyCrossed(LogicalPlanUtil.RowKeyRange range1, LogicalPlanUtil.RowKeyRange range2) {
        if (Bytes.compareTo(range1.getStartRowKey(), range2.getEndRowKey()) > 0 ||
                Bytes.compareTo(range1.getEndRowKey(), range2.getStartRowKey()) < 0)
            return false;
        return true;
    }

    private boolean isScanCrossed(ScanWithPlan swp1, ScanWithPlan swp2) {
        RowKeyRange range1 = getRowKeyRange(swp1.scan);
        RowKeyRange range2 = getRowKeyRange(swp2.scan);
        return isRowKeyCrossed(range1, range2);
    }

    public static boolean isRkRangeInScan(byte[] rkPoint, ScanWithPlan swp) {
        RowKeyRange scanRange = swp.range;
        byte[] scanSrk = scanRange.getStartRowKey(), scanEnk = scanRange.getEndRowKey();
        if (Bytes.compareTo(scanEnk, rkPoint) >= 0 && Bytes.compareTo(scanSrk, rkPoint) < 0) {
            return true;
        }
        return false;
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

    public static Scan transferHBaseScan(Scan scan, DrillConfig config) throws Exception {
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
        return scan;
    }

    private static RowKeyRange getRkTailRange(JsonNode filterNode, String tableName, DrillConfig config) throws IOException {
        Map<String, UnitFunc> fieldFunc = parseFilterExpr(filterNode, config);
        UnitFunc tailFunc = fieldFunc.get("uid");
        if (tailFunc == null)
            return null;
        FunctionCall call = tailFunc.getFunc();
        byte[] srt = null, end = null;
        List<UnitFunc> funcs = parseToUnit(call, config);
        for (UnitFunc func : funcs) {
            String funcName = func.getOp();
            if (funcName.contains("greater")) {
                srt = Bytes.tail(Bytes.toBytes(Long.parseLong(func.getValue())), 5);
            } else if (funcName.contains("less")) {
                end = Bytes.tail(Bytes.toBytes(Long.parseLong(func.getValue())), 5);
            }
        }
        return new RowKeyRange(Bytes.toStringBinary(srt), Bytes.toStringBinary(end));
    }

    public static RowKeyRange getRowKeyRangeFromFilter(JsonNode selectionNode, String tableName, DrillConfig config) throws Exception {
        JsonNode filterNode = selectionNode.get(SELECTION_KEY_WORD_FILTER).get(QueryMasterConstant.EXPRESSION);
        return getDeuRkRange(tableName, filterNode, config);
    }

  /**
   * 通过解析filter的表达式获取针对DEU table的start row key 和 end row key
   * @param tableName
   * @param filter
   * @param config
   * @return
   * @throws Exception
   */
    public static RowKeyRange getDeuRkRange(String tableName, JsonNode filter, DrillConfig config) throws Exception {
      List<KeyPart> kps = MetaUtil.getInstance().getTableRkKps(tableName);
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
          srk = date + eventFrom + QueryMasterConstant.START_KEY_TAIL;
          erk = date + eventTo + QueryMasterConstant.END_KEY_TAIL;
        }else {
          srk = date + QueryMasterConstant.NA_START_KEY;
          erk = date + QueryMasterConstant.NA_END_KEY;
        }
      } else {
        //已经是具体事件
        srk = date + eventFilter + QueryMasterConstant.START_KEY_TAIL;
        erk = date + eventFilter + QueryMasterConstant.END_KEY_TAIL;
      }

      return new RowKeyRange(srk, erk);
    }

    public static Map<String, UnitFunc> parseFilterExpr(JsonNode origExpr, DrillConfig config) throws IOException {
        LogicalExpression func = config.getMapper().readValue(origExpr.traverse(), LogicalExpression.class);
        return parseFunctionCall((FunctionCall) func, config);
    }

    public static Map<String, UnitFunc> parseFunctionCall(FunctionCall func, DrillConfig config) {
        Map<String, UnitFunc> result = new HashMap<>();
        String field = null;
        UnitFunc value = null;
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
            } else if (le instanceof ValueExpressions.QuotedString) {
                value = new UnitFunc(func);
            }
        }
        if (field != null && value != null) {
            if (result.containsKey(field)) {
                LogicalExpression old = result.get(field).getFunc();
                FunctionRegistry registry = new FunctionRegistry(config);
                FunctionCall call = (FunctionCall) registry.createExpression("&&", ExpressionPosition.UNKNOWN, Arrays.asList(old, value.getFunc()));
                UnitFunc resultFunc = new UnitFunc(call);
                result.put(field, resultFunc);
            } else
                result.put(field, value);
        }
        return result;
    }

    public static List<UnitFunc> parseToUnit(FunctionCall call, DrillConfig config) {
        List<UnitFunc> result = new ArrayList<>();
        for (LogicalExpression le : call) {
            if (le instanceof FunctionCall) {
                result.addAll(parseToUnit((FunctionCall) le, config));
            } else {
                result.add(new UnitFunc(call));
            }
        }
        return result;
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

        public boolean equals(Object o){
            if (!( o instanceof UnitFunc))
                return false;
            if(func.equals(((UnitFunc)o).getFunc()))
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
            this.startRowKey = ByteUtils.toBytesBinary(srk);
            this.endRowKey = ByteUtils.toBytesBinary(enk);
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

  public static LogicalPlan getRkRangePlan(LogicalPlan plan, DrillConfig config) throws Exception {
    Collection<SourceOperator> operators = plan.getGraph().getLeaves();
    long t1,t2;
    for(SourceOperator operator: operators){
      if(operator instanceof Scan){
        logger.info("start transfer scan ");
        t1 = System.currentTimeMillis();
        transferHBaseScan((Scan)operator,config);
        t2=System.currentTimeMillis();
        logger.info("transfer hbase scan using "+(t2-t1)+" ms");
        logger.info("transfer scan complete");
      }
    }
    return plan;
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


}
