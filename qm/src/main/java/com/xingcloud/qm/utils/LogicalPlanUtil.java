package com.xingcloud.qm.utils;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.NullNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.xingcloud.events.XEventOperation;
import com.xingcloud.events.XEventRange;
import com.xingcloud.meta.HBaseFieldInfo;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import com.xingcloud.qm.enums.Operator;
import com.xingcloud.qm.result.ResultTable;
import com.xingcloud.qm.service.LOPComparator;
import com.xingcloud.qm.service.PlanMerge;
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
    public static Logger logger=Logger.getLogger(LogicalPlanUtil.class);
    public static String trimSingleQuote(String rkStr) {
        if (rkStr.startsWith("'"))
            rkStr = rkStr.substring(1);
        if (rkStr.endsWith("'"))
            rkStr = rkStr.substring(0, rkStr.length() - 1);
        return rkStr;
    }

    public static String addSingleQuote(String rkStr) {
        rkStr = rkStr.concat("'");
        rkStr = new String("'").concat(rkStr);
        return rkStr;
    }

    public static List<Map<String, Object>> getSelectionMap(Scan scan) {
        List<Map<String, Object>> selectionMapList = new ArrayList<>();

        JsonNode selections = scan.getSelection().getRoot();
        for (JsonNode selectionRoot : selections) {
            Map<String, Object> selectionMap = new HashMap<>();
            //projection
            JsonNode projections = selectionRoot.get(Selections.SELECTION_KEY_WORD_PROJECTIONS);
            List<Object> projectionFields = new ArrayList<>();
            for (JsonNode node : projections) {
                Map<String, Object> projectionField = new HashMap<>();
                String ref = node.get("ref").textValue();
                String expr = node.get("expr").textValue();
                projectionField.put("ref", ref);
                projectionField.put("expr", expr);
                projectionFields.add(projectionField);
            }
            selectionMap.put(Selections.SELECTION_KEY_WORD_PROJECTIONS, projectionFields);

            //filter
            /*
            JsonNode filters = selectionRoot.get(Selections.SELECTION_KEY_WORD_FILTERS);
            if (filters != null && !(filters instanceof NullNode)) {
                List<Object> filterList = new ArrayList<>();
                for (JsonNode node : filters) {
                    Map<String, Object> filterFieldMap = new HashMap<>();
                    String filterType = node.get(Selections.SELECTION_KEY_WORD_FILTER_TYPE).textValue();
                    filterFieldMap.put(Selections.SELECTION_KEY_WORD_FILTER_TYPE, filterType);
                    JsonNode includes = node.get(Selections.SELECTION_KEY_WORD_ROWKEY_INCLUDES);
                    List<Object> exprs = new ArrayList<>();
                    for (JsonNode node1 : includes) {
                        exprs.add(node1.textValue());
                    }
                    filterFieldMap.put(Selections.SELECTION_KEY_WORD_ROWKEY_INCLUDES, exprs);
                    List<Object> mappingExprs = new ArrayList<>();
                    JsonNode mapping = node.get(Selections.SELECTION_KEY_WORD_ROWKEY_EVENT_MAPPING);
                    for (JsonNode node1 : mapping) {
                        mappingExprs.add(node1.textValue());
                    }
                    filterFieldMap.put(Selections.SELECTION_KEY_WORD_ROWKEY_EVENT_MAPPING, mappingExprs);
                    filterList.add(filterFieldMap);
                }
                selectionMap.put(Selections.SELECTION_KEY_WORD_FILTERS, filterList);
            }
            */
            JsonNode filter = selectionRoot.get(Selections.SELECTION_KEY_WORD_FILTER);

            if (filter != null && !(filter instanceof NullNode)) {
                Map<String,Object> filterMap=new HashMap<>();
                filterMap.put("type",filter.get("type").asText());
                filterMap.put("expression",filter.get("expression"));
                selectionMap.put(Selections.SELECTION_KEY_WORD_FILTER, filterMap);
            }
            //selectionMap.put(SELECTION_KEY_WORD_FILTER, selectionRoot.get(SELECTION_KEY_WORD_FILTER));
            //rowkey
            JsonNode rowkeyRange = selectionRoot.get(Selections.SELECTION_KEY_WORD_ROWKEY);
            Map<String, Object> rkMap = new HashMap<>();
            rkMap.put(Selections.SELECTION_KEY_WORD_ROWKEY_START,
                    rowkeyRange.get(Selections.SELECTION_KEY_WORD_ROWKEY_START).textValue());
            rkMap.put(Selections.SELECTION_KEY_WORD_ROWKEY_END,
                    rowkeyRange.get(Selections.SELECTION_KEY_WORD_ROWKEY_END).textValue());
            selectionMap.put(Selections.SELECTION_KEY_WORD_ROWKEY, rkMap);
            //tail
            JsonNode tailRange= selectionRoot.get(SELECTION_KEY_ROWKEY_TAIL_RANGE);
            if(tailRange!=null && !(tailRange instanceof NullNode)){
                Map<String,Object> tailRangeMap=new HashMap<>();
                tailRangeMap.put(SELECTION_KEY_ROWKEY_TAIL_START,tailRange.get(SELECTION_KEY_ROWKEY_TAIL_START).toString());
                tailRangeMap.put(SELECTION_KEY_ROWKEY_TAIL_END,tailRange.get(SELECTION_KEY_ROWKEY_TAIL_END).toString());
                selectionMap.put(SELECTION_KEY_ROWKEY_TAIL_RANGE,tailRangeMap);
            }
            //table
            selectionMap.put(Selections.SELECTION_KEY_WORD_TABLE,
                    selectionRoot.get(Selections.SELECTION_KEY_WORD_TABLE));

            selectionMapList.add(selectionMap);
        }
        return selectionMapList;
    }

    public static JSONOptions buildJsonOptions(List<Map<String, Object>> mapList, DrillConfig config) throws IOException {
        ObjectMapper mapper = config.getMapper();
        String optionStr = mapper.writeValueAsString(mapList);
        JSONOptions options = mapper.readValue(optionStr, JSONOptions.class);
        return options;
    }

    public static Scan getBaseScan(RowKeyRange range, List<ScanWithPlan> swps, DrillConfig config) throws IOException {

        List<JsonNode> selectionNodeList = new ArrayList<>();
        for (ScanWithPlan swp : swps) {
            for(JsonNode selectionNode : swp.scan.getSelection().getRoot()){
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

        List<List<String>> filterFields = new ArrayList<>();
        List<LogicalExpression> baseFilterExprs;
        Set<LogicalExpression> baseFilterExprSet=new HashSet<>();
        List<String> baseFilterFields=new ArrayList<>();
        List<Object> projections = new ArrayList<>();
        List<String> projectionExprNames = new ArrayList<>(),projectionRefNames = new ArrayList<>();

        boolean needFilter = true;
        String filterType=null;
        for (JsonNode selectionNode : selectionNodeList) {
            JsonNode ProjectionsNode =
                     selectionNode.get(SELECTION_KEY_WORD_PROJECTIONS);
            for ( JsonNode projection : ProjectionsNode) {
                String exprName = projection.get("expr").textValue();
                String refName = projection.get("ref").textValue();
                if (projectionExprNames.contains(exprName) && projectionRefNames.contains(refName))
                    continue;
                projectionExprNames.add(exprName);
                projectionRefNames.add(refName);
                projections.add(projection);
            }

            JsonNode filterNode = selectionNode.get(SELECTION_KEY_WORD_FILTER);
            if (filterNode == null || filterNode instanceof NullNode) {
                needFilter = false;
                continue;
            }
            filterType=filterNode.get("type").textValue();
            baseFilterExprSet.add(config.getMapper().readValue((filterNode.get("expression")).traverse(),LogicalExpression.class));
            Map<String, UnitFunc> filterFuncMap = parseFilterExpr(filterNode.get("expression"), config);
            filterFields.add(new ArrayList<>(filterFuncMap.keySet()));
        }


        if (needFilter) {
            FunctionRegistry registry = new FunctionRegistry(config);
            baseFilterExprs = new ArrayList<>(baseFilterExprSet);
            LogicalExpression filterExpr = baseFilterExprs.size() > 1 ? registry.createExpression("or", ExpressionPosition.UNKNOWN, baseFilterExprs) : baseFilterExprs.get(0);
            Map<String, Object> filter = new HashMap<>();
            filter.put("type", filterType);
            filter.put("expression", filterExpr);
            selectionMap.put(SELECTION_KEY_WORD_FILTER, filter);

            baseFilterFields.addAll(filterFields.get(0));
            for (int i = 1; i < filterFields.size(); i++) {
                for (String field : baseFilterFields) {
                    if (!filterFields.get(i).contains(field))
                        filterFields.get(0).remove(field);
                }
                baseFilterFields.clear();
                baseFilterFields.addAll(filterFields.get(0));
            }

        }
        for (List<String> fields : filterFields) {
            for (String field : fields) {
                if (!baseFilterFields.contains(field)) {
                    if (projectionExprNames.contains(field) && projectionRefNames.contains(field))
                        continue;
                    Map<String, Object> projectionMap = new HashMap<>();
                    projections.add(projectionMap);
                    projectionMap.put("ref", field);
                    projectionMap.put("expr",field);
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

    /*
    public static List<LogicalExpression> getFilterEntry(Scan scan, DrillConfig config) throws IOException {
        JsonNode filters = scan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_FILTERS);
        if (filters == null || filters instanceof NullNode) {
            return null;
        }
        List<LogicalExpression> les = new ArrayList<>();
        for (JsonNode filter : filters) {
            JsonNode mapping = filter.get(Selections.SELECTION_KEY_WORD_ROWKEY_EVENT_MAPPING);
            if (mapping == null || mapping instanceof NullNode) {
                return null;
            }
            for (JsonNode expr : mapping) {
                LogicalExpression le = config.getMapper().readValue(expr.toString(), LogicalExpression.class);
                les.add(le);
            }
        }
        return les;
    }
    */
    public static Filter getFilter(Scan baseScan, Scan scan, DrillConfig config) throws IOException {
        //List<LogicalExpression> filterEntry = getFilterEntry(scan, config);
        List<LogicalExpression> filterEntry = getFilterEntry(baseScan, scan, config);
        if (filterEntry == null) {
            return null;
        }
        //ExpressionPosition position = new ExpressionPosition(null, 0);
        FunctionRegistry registry = new FunctionRegistry(config);

        LogicalExpression filterExpr = registry.createExpression("&&", ExpressionPosition.UNKNOWN, filterEntry);
        //LogicalExpression filterExpr=registry.createExpression("and",position,filterEntry);
        Filter filter = new Filter(filterExpr);
        return filter;
    }

    public static List<LogicalExpression> getFilterEntry(Scan baseScan, Scan scan, DrillConfig config) throws IOException {
        JsonNode baseFilterExpr = baseScan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_FILTER).get("expression");
        JsonNode fitlerExpr = scan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_FILTER).get("expression");
        Map<String, UnitFunc> filterFuncMap = parseFilterExpr(fitlerExpr, config);
        Map<String, UnitFunc> baseFilterFuncMap = parseFilterExpr(baseFilterExpr, config);
        if(baseFilterFuncMap.size()>=filterFuncMap.size())return null;
        List<LogicalExpression> exprs = new ArrayList<>();
        for (Map.Entry<String, UnitFunc> entry : baseFilterFuncMap.entrySet()) {
            if (filterFuncMap.containsKey(entry.getKey()))
                filterFuncMap.remove(entry.getKey());
        }
        for (Map.Entry<String, UnitFunc> entry : filterFuncMap.entrySet()) {
            LogicalExpression le = entry.getValue().getFunc();
            exprs.add(le);
        }
        //if (exprs.size() == 0) return null;
        return exprs;
    }

    public List<String> getFields(JsonNode filterNode, DrillConfig config) throws IOException {
        try {
            return getFields((LogicalExpression)config.getMapper().readValues(filterNode.get("expression").traverse(),LogicalExpression.class),config);
        } catch (IOException e) {
            e.printStackTrace();
            throw e;
        }
    }
    public List<String> getFields(LogicalExpression filterExpr,DrillConfig config){
        Set<String> fields=new HashSet<>();
        if(!(filterExpr instanceof FunctionCall))
            return null;
        for(LogicalExpression childExpr : (FunctionCall)filterExpr){
            if(childExpr instanceof SchemaPath){
                fields.add(((SchemaPath)childExpr).getPath().toString());
            }else if(childExpr instanceof FunctionCall)
                 fields.addAll(getFields(childExpr,config));
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
        //JsonNode projectionNode=scan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_PROJECTIONS);
        //List<Map<String, Object>> selectionList = getSelectionMap(scan);
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

    public static boolean isRkRangeInScan(RowKeyRange range, ScanWithPlan swp) {
        RowKeyRange scanRange = LogicalPlanUtil.getRowKeyRange(swp.scan);
        byte[] scanSrk = scanRange.getStartRowKey(), scanEnk = scanRange.getEndRowKey();
        byte[] srk = range.getStartRowKey(), enk = range.getEndRowKey();
        if (Bytes.compareTo(srk, scanSrk) >= 0 && Bytes.compareTo(enk, scanEnk) <= 0)
            return true;
        return false;
    }

    public static boolean isRangeEquals(RowKeyRange range1, RowKeyRange range2) {
        if (Bytes.compareTo(range1.getStartRowKey(), range2.getStartRowKey()) == 0 &&
                Bytes.compareTo(range1.getEndRowKey(), range2.getEndRowKey()) == 0)
            return true;
        return false;
    }


    private boolean childrenSame(LogicalOperator op1, LogicalOperator op2) {
        Iterator<LogicalOperator> iter1 = op1.iterator();
        Iterator<LogicalOperator> iter2 = op2.iterator();
        for (; iter1.hasNext(); ) {
            if (!iter2.hasNext()) {
                return false;
            }
            if (!iter1.next().equals(iter2.next())) {
                return false;
            }
        }
        return true;
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
            if(scan.getSelection().getRoot() instanceof ArrayNode){
                return scan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_TABLE).asText();
            }else {
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
            for(JsonNode selectionNode : scan.getSelection().getRoot()){
                RowKeyRange range =getRowKeyRangeFromFilter(selectionNode,tableName,config);
                ObjectNode rkRangeNode=new ObjectNode(JsonNodeFactory.instance);
                rkRangeNode.put(SELECTION_KEY_WORD_ROWKEY_START,range.getStartRkStr());
                rkRangeNode.put(SELECTION_KEY_WORD_ROWKEY_END,range.getEndRkStr());
                ((ObjectNode)selectionNode).put(SELECTION_KEY_WORD_ROWKEY,rkRangeNode);
            }
        }
        return scan;
    }

    public static RowKeyRange getRkTailRangeFromFilter(Map<String,Object> selection,String tableName,DrillConfig config) throws IOException {
        Map<String,Object> filterMap=(Map<String,Object>)selection.get(SELECTION_KEY_WORD_FILTER);
        return getRkTailRange((JsonNode)filterMap.get("expression"),tableName,config);
    }

    private static RowKeyRange getRkTailRange(JsonNode filterNode, String tableName, DrillConfig config) throws IOException {
        Map<String,UnitFunc> fieldFunc=parseFilterExpr(filterNode,config);
        UnitFunc tailFunc=fieldFunc.get("uid");
        if(tailFunc==null)
            return null;
        FunctionCall call=tailFunc.getFunc();
        byte[] srt=null,end=null;
        List<UnitFunc> funcs=parseToUnit(call,config);
        for(UnitFunc func : funcs){
            String funcName=func.getOp();
            if(funcName.contains("greater")){
               srt=Bytes.tail(Bytes.toBytes(Long.parseLong(func.getValue())), 5);
            }else if(funcName.contains("less")){
               end=Bytes.tail(Bytes.toBytes(Long.parseLong(func.getValue())),5);
            }
        }
        return new RowKeyRange(Bytes.toStringBinary(srt),Bytes.toStringBinary(end));
    }

    public static RowKeyRange getRowKeyRangeFromFilter(JsonNode selectionNode, String tableName, DrillConfig config) throws Exception {
         JsonNode filterNode=selectionNode.get(SELECTION_KEY_WORD_FILTER).get("expression");
         return getRkRange(tableName,filterNode,config);
    }

    public static RowKeyRange getRkRange(String tableName,JsonNode filter,DrillConfig config) throws Exception {
        List<KeyPart> kps = MetaUtil.getInstance().getTableRkKps(tableName);
        String srkHead = "", enkHead = "";
        String event = "";
        Map<String, UnitFunc> fieldFunc = parseFilterExpr(filter, config);

        List<KeyPart> workKps = kps;
        Deque<KeyPart> toWorkKps = new ArrayDeque<>(workKps);
        //String tail=null;
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
                    if (kp.getField().getName().contains("event")) {
                        event += value;
                    } else {
                        if (event.length() > 0)
                            break loop;
                        srkHead += value;
                        enkHead += value;
                    }
                    toWorkKps.removeFirst();
                } else if (kp.getType() == KeyPart.Type.constant) {
                    if (kp.getConstant().equals("\\xFF")) break loop;
                    if (event.length() != 0) {
                        event += kp.getConstant();
                    }
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
        if(event.endsWith("."))
            event=event.substring(0,event.length()-1);
        String projectId = null;
        if (tableName.contains("_deu"))
            projectId = tableName.replaceAll("_deu", "");
        else if (tableName.contains("deu_"))
            projectId = tableName.replace("deu_", "");
        long t1=System.currentTimeMillis(),t2;
        XEventRange range = XEventOperation.getInstance().getEventRange(projectId, event);
        t2=System.currentTimeMillis();
        logger.info("get event "+event+" using "+(t2-t1)+" ms");
        String eventFrom = "";
        for (int i = 0; i < range.getFrom().getEventArray().length; i++) {
            String levelEvent = range.getFrom().getEventArray()[i];
            if (levelEvent == null)
                break;
            eventFrom += levelEvent + ".";
        }
        String eventTo = "";
        for (int i = 0; i < range.getTo().getEventArray().length; i++) {
            String levelEvent = range.getTo().getEventArray()[i];
            if (levelEvent == null)
                break;
            eventTo += levelEvent + ".";
        }
        String srk = srkHead + eventFrom+"\\xFF";
        String enk = enkHead + eventTo+"\\xFF";
        return new RowKeyRange(srk, enk);
    }

    public static Map<String, UnitFunc> parseFilterExpr(JsonNode origExpr, DrillConfig config) throws IOException {
        Map<String, UnitFunc> resultMap = new HashMap<>();
        LogicalExpression func = config.getMapper().readValue(origExpr.traverse(), LogicalExpression.class);
        return parseFunctionCall((FunctionCall) func,config);
        /*
        String[] exprs = origExpr.split("&&");
        for (String expr : exprs) {
            UnitFunc result = new UnitFunc();
            String[] args = new String[2];
            if (expr.contains("=")) {
                result.setOp("=");
                args = expr.split("=");
            } else if (expr.contains(">")) {
                result.setOp(">");
                args = expr.split(">");
            } else if (expr.contains("<")) {
                result.setOp("<");
                args = expr.split("<");
            }
            result.setField(args[0].trim());
            result.setValue(trimSingleQuote(args[1].trim()));
            resultMap.put(result.getField(), result);
        }
        */
        //return resultMap;
    }

    public static Map<String, UnitFunc> parseFunctionCall(FunctionCall func,DrillConfig config) {
        Map<String, UnitFunc> result = new HashMap<>();
        String field = null;
        UnitFunc value = null;
        for (LogicalExpression le : func) {
            if (le instanceof FunctionCall) {
                for(Map.Entry<String,UnitFunc> entry: parseFunctionCall(((FunctionCall) le),config).entrySet()){
                      if(result.containsKey(entry.getKey())){
                          LogicalExpression old=result.get(entry.getKey()).getFunc();
                          FunctionRegistry registry=new FunctionRegistry(config);
                          FunctionCall call=(FunctionCall)registry.createExpression("&&",ExpressionPosition.UNKNOWN,Arrays.asList(old, entry.getValue().getFunc()));
                          UnitFunc resultFunc=new UnitFunc(call);
                          result.put(field,resultFunc);
                      }else
                          result.put(entry.getKey(),entry.getValue());
                }
            } else if (le instanceof SchemaPath) {
                field = ((SchemaPath) le).getPath().toString();
            } else if (le instanceof ValueExpressions.QuotedString) {
                value = new UnitFunc(func);
            }
        }
        if (field != null && value != null) {
            if(result.containsKey(field)){
                LogicalExpression old=result.get(field).getFunc();
                FunctionRegistry registry=new FunctionRegistry(config);
                FunctionCall call=(FunctionCall)registry.createExpression("&&",ExpressionPosition.UNKNOWN,Arrays.asList(old,value.getFunc()));
                UnitFunc resultFunc=new UnitFunc(call);
                result.put(field,resultFunc);
            }
            else
                result.put(field, value);
        }
        return result;
    }

    public static List<UnitFunc> parseToUnit(FunctionCall call,DrillConfig config){
        List<UnitFunc> result=new ArrayList<>();
        for(LogicalExpression le : call){
            if(le instanceof  FunctionCall){
                result.addAll(parseToUnit((FunctionCall)le,config));
            }else{
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
    }

    public static List<Map<String, Object>> getOrigSelectionMap(Scan scan) {
        List<Map<String, Object>> selectionMapList = new ArrayList<>();

        JsonNode selections = scan.getSelection().getRoot();
        for (JsonNode selectionRoot : selections) {
            Map<String, Object> selectionMap = new HashMap<>();
            //projection
            JsonNode projections = selectionRoot.get(Selections.SELECTION_KEY_WORD_PROJECTIONS);
            List<Object> projectionFields = new ArrayList<>();
            for (JsonNode node : projections) {
                Map<String, Object> projectionField = new HashMap<>();
                String ref = node.get("ref").textValue();
                String expr = node.get("expr").textValue();
                projectionField.put("ref", ref);
                projectionField.put("expr", expr);
                projectionFields.add(projectionField);
            }
            selectionMap.put(Selections.SELECTION_KEY_WORD_PROJECTIONS, projectionFields);

            //filter
            JsonNode filter = selectionRoot.get(Selections.SELECTION_KEY_WORD_FILTER);

            if (filter != null && !(filter instanceof NullNode)) {
                Map<String,Object> filterMap=new HashMap<>();
                filterMap.put("type",filter.get("type").asText());
                filterMap.put("expression",filter.get("expression"));
                selectionMap.put(Selections.SELECTION_KEY_WORD_FILTER, filterMap);
            }
            //table
            selectionMap.put(Selections.SELECTION_KEY_WORD_TABLE,
                    selectionRoot.get(Selections.SELECTION_KEY_WORD_TABLE));

            selectionMapList.add(selectionMap);
        }
        return selectionMapList;
    }

    public static enum MergeType {
        same, belongsto, sametable
    }

    public static class RowKeyRange {
        byte[] startRowKey, endRowKey;
        String startRkStr,endRkStr;
        public RowKeyRange(String srk, String enk) {
            this.startRowKey = ByteUtils.toBytesBinary(srk);
            this.endRowKey = ByteUtils.toBytesBinary(enk);
            this.startRkStr=srk;
            this.endRkStr=enk;
        }

        public byte[] getStartRowKey() {
            return startRowKey;
        }

        public byte[] getEndRowKey() {
            return endRowKey;
        }

        public String getStartRkStr(){
           return startRkStr;
        }
        public String getEndRkStr(){
            return endRkStr;
        }

        public String toString() {
            return "srk: " + ByteUtils.toStringBinary(startRowKey) + "\n"
                    + "enk: " + ByteUtils.toStringBinary(endRowKey);
        }
    }

    public static class RowKeyRangeComparator implements Comparator<RowKeyRange> {
        @Override
        public int compare(RowKeyRange o1, RowKeyRange o2) {
            return Bytes.compareTo(o1.getStartRowKey(), o2.getStartRowKey());
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


}
