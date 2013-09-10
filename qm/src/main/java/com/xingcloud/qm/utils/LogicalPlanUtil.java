package com.xingcloud.qm.utils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.node.NullNode;
import com.xingcloud.basic.utils.Pair;
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
    public static String trimSingleQuote(String rkStr){
        if(rkStr.startsWith("'"))
           rkStr=rkStr.substring(1);
        if(rkStr.endsWith("'"))
            rkStr=rkStr.substring(0,rkStr.length()-1);
        return rkStr;
    }

    public static String addSingleQuote(String rkStr){
        rkStr=rkStr.concat("'");
        rkStr=new String("'").concat(rkStr);
        return rkStr;
    }
    public static List<Map<String,Object>> getSelectionMap(Scan scan){
         List<Map<String,Object>> selectionMapList=new ArrayList<>();

         JsonNode selections=scan.getSelection().getRoot();
         for(JsonNode selectionRoot: selections){
            Map<String,Object> selectionMap=new HashMap<>();
         //projection
            JsonNode projections=selectionRoot.get(Selections.SELECTION_KEY_WORD_PROJECTIONS);
            List<Object> projectionFields=new ArrayList<>();
            for(JsonNode node: projections){
                Map<String,Object> projectionField=new HashMap<>();
                String ref=node.get("ref").toString();
                String expr=node.get("expr").toString();
                projectionField.put("ref",ref);
                projectionField.put("expr",expr);
                projectionFields.add(projectionField);
            }
            selectionMap.put(Selections.SELECTION_KEY_WORD_PROJECTIONS,projectionFields);

         //filter
            JsonNode filters=selectionRoot.get(Selections.SELECTION_KEY_WORD_FILTERS);
            if(filters!=null && !(filters instanceof NullNode)){
                List<Object> filterList=new ArrayList<>();
                for(JsonNode node: filters){
                    Map<String,Object> filterFieldMap=new HashMap<>();
                    String filterType=node.get(Selections.SELECTION_KEY_WORD_FILTER_TYPE).toString();
                    filterFieldMap.put(Selections.SELECTION_KEY_WORD_FILTER_TYPE,filterType);
                    JsonNode includes=node.get(Selections.SELECTION_KEY_WORD_ROWKEY_INCLUDES);
                    List<Object> exprs=new ArrayList<>();
                    for(JsonNode node1: includes){
                        exprs.add(node1.toString());
                    }
                    filterFieldMap.put(Selections.SELECTION_KEY_WORD_ROWKEY_INCLUDES,exprs);
                    List<Object> mappingExprs=new ArrayList<>();
                    JsonNode mapping=node.get(Selections.SELECTION_KEY_WORD_ROWKEY_EVENT_MAPPING);
                    for(JsonNode node1: mapping){
                        mappingExprs.add(node1.toString());
                    }
                    filterFieldMap.put(Selections.SELECTION_KEY_WORD_ROWKEY_EVENT_MAPPING,mappingExprs);
                    filterList.add(filterFieldMap);
                }
                selectionMap.put(Selections.SELECTION_KEY_WORD_FILTERS, filterList);
            }
         //rowkey
            JsonNode rowkeyRange=selectionRoot.get(Selections.SELECTION_KEY_WORD_ROWKEY);
            Map<String,Object> rkMap=new HashMap<>();
            rkMap.put(Selections.SELECTION_KEY_WORD_ROWKEY_START,
                 rowkeyRange.get(Selections.SELECTION_KEY_WORD_ROWKEY_START).toString());
            rkMap.put(Selections.SELECTION_KEY_WORD_ROWKEY_END,
                 rowkeyRange.get(Selections.SELECTION_KEY_WORD_ROWKEY_END).toString());
            selectionMap.put(Selections.SELECTION_KEY_WORD_ROWKEY,rkMap);
         //table
            selectionMap.put(Selections.SELECTION_KEY_WORD_TABLE,
                 selectionRoot.get(Selections.SELECTION_KEY_WORD_TABLE));

            selectionMapList.add(selectionMap);
            }
         return selectionMapList;
    }

    public static JSONOptions buildJsonOptions(List<Map<String,Object>> mapList,DrillConfig config) throws IOException {
        ObjectMapper mapper=config.getMapper();
        String optionStr=mapper.writeValueAsString(mapList);
        JSONOptions options=mapper.readValue(optionStr,JSONOptions.class);
        return options;
    }

    public static Scan getBaseScan(RowKeyRange range,List<ScanWithPlan> swps,DrillConfig config) throws IOException {
        String storageEngine =swps.get(0).scan.getStorageEngine();
        List<Map<String,Object>> selctionMapList= LogicalPlanUtil.getSelectionMap(swps.get(0).scan);
        Map<String,Object> selectionMap=selctionMapList.get(0);

        Map<String,Object> rkMap=(Map<String,Object>)selectionMap.get(SELECTION_KEY_WORD_ROWKEY);
        rkMap.remove(Selections.SELECTION_KEY_WORD_ROWKEY_START);
        rkMap.remove(SELECTION_KEY_WORD_ROWKEY_END);
        rkMap.put(SELECTION_KEY_WORD_ROWKEY_START, ByteUtils.toStringBinary(range.getStartRowKey()));
        rkMap.put(SELECTION_KEY_WORD_ROWKEY_END,ByteUtils.toStringBinary(range.getEndRowKey()));
        List<Map<String,Object>> baseFilters=new ArrayList<>();
        List<NamedExpression> addePprojections=new ArrayList<>();
        if(swps.size()!=1){
            boolean needFilter=true;
            List<String> patterns=new ArrayList<>();


            for(int i=0;i<swps.size();i++){
                JSONOptions selection=swps.get(i).scan.getSelection();
                JsonNode selectionNode=selection.getRoot().get(0);
                JsonNode filters=selectionNode.get(SELECTION_KEY_WORD_FILTERS);
                if(filters instanceof NullNode || filters==null){
                    needFilter=false;
                    continue;
                }
                for(JsonNode filter : filters){
                    String type=filter.get(SELECTION_KEY_WORD_FILTER_TYPE).toString();
                    if(type.contains("ROWKEY")){
                        JsonNode patternIncludes=filter.get(Selections.SELECTION_KEY_WORD_ROWKEY_INCLUDES);
                        for(JsonNode pattern: patternIncludes){
                            if(!patterns.contains(pattern.textValue()))
                                patterns.add(pattern.textValue());
                        }
                        JsonNode mappings=filter.get(SELECTION_KEY_WORD_ROWKEY_EVENT_MAPPING);
                        for(JsonNode mapping: mappings){
                            String mapStr=mapping.toString();
                            LogicalExpression le=config.getMapper().readValue(mapStr,LogicalExpression.class);
                            String refName=((SchemaPath)((FunctionCall)le).args.get(0)).getPath().toString();
                            FieldReference ref=new FieldReference(refName,le.getPosition());
                            LogicalExpression expr=ref;
                            NamedExpression ne=new NamedExpression(expr,ref);
                            if(!addePprojections.contains(ne))addePprojections.add(ne);
                        }
                    }else {
                        JsonNode includes=filter.get(SELECTION_KEY_WORD_ROWKEY_INCLUDES);
                        for(JsonNode filterExpr : includes){
                            LogicalExpression le=config.getMapper().readValue(filterExpr.toString(),LogicalExpression.class);
                            String refName=((SchemaPath)((FunctionCall)le).args.get(0)).getPath().toString();
                            FieldReference ref=new FieldReference(refName,le.getPosition());
                            LogicalExpression expr=ref;
                            NamedExpression ne=new NamedExpression(expr,ref);
                            if(!addePprojections.contains(ne))
                                addePprojections.add(ne);
                        }
                    }
                }
                //JsonNode patternFilter=filters.get(0);

            }
            if(needFilter){
                List<Map<String,Object>> filterList=(List<Map<String,Object>>)
                    selectionMap.get(Selections.SELECTION_KEY_WORD_FILTERS);
                filterList.get(0).remove(Selections.SELECTION_KEY_WORD_ROWKEY_INCLUDES);
                filterList.get(0).remove(Selections.SELECTION_KEY_WORD_ROWKEY_EVENT_MAPPING);
                filterList.get(0).put(SELECTION_KEY_WORD_ROWKEY_INCLUDES, patterns);
                filterList.get(0).put(SELECTION_KEY_WORD_ROWKEY_EVENT_MAPPING, null);
                baseFilters.add(filterList.get(0));
            }
        }

        if(addePprojections.size()>0){
            Map<String,Object> selctionNode=selctionMapList.get(0);
            List<Map<String,Object>> projections=(List<Map<String,Object>>)
                    selctionNode.get(SELECTION_KEY_WORD_PROJECTIONS);
            for(NamedExpression ne: addePprojections){
                Map<String,Object> projection=new HashMap<>();
                projection.put("ref",config.getMapper().writeValueAsString(ne.getRef()));
                projection.put("expr",config.getMapper().writeValueAsString(ne.getExpr()));
                projections.add(projection);
            }
        }

        JSONOptions selection= LogicalPlanUtil.buildJsonOptions(selctionMapList, config);
        FieldReference ref=swps.get(0).scan.getOutputReference();
        Scan scan=new Scan(storageEngine,selection,ref);
        return scan;
    }

    public static List<LogicalExpression> getFilterEntry(Scan scan,DrillConfig config) throws IOException {
        JsonNode filters=scan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_FILTERS);
        if(filters==null || filters instanceof NullNode){
            return null;
        }
        List<LogicalExpression> les=new ArrayList<>();
        for(JsonNode filter: filters){
            JsonNode mapping=filter.get(Selections.SELECTION_KEY_WORD_ROWKEY_EVENT_MAPPING);
            if(mapping==null || mapping instanceof  NullNode){
                return null;
            }
            for(JsonNode expr: mapping){
                LogicalExpression le=config.getMapper().readValue(expr.toString(),LogicalExpression.class);                les.add(le);
            }
        }
        return les;
    }

    public static Filter getFilter(Scan scan,DrillConfig config) throws IOException {
        List<LogicalExpression> filterEntry=getFilterEntry(scan,config);
        if(filterEntry==null){
            return null;
        }
        ExpressionPosition position=new ExpressionPosition(null,0);
        FunctionRegistry registry=new FunctionRegistry(config);
        LogicalExpression filterExpr=registry.createExpression("and",position,filterEntry);
        Filter filter=new Filter(filterExpr);
        return filter;
    }

    public static Project getProject(Scan scan,DrillConfig config) throws IOException{
        List<NamedExpression> nes=getProjectionEntry(scan,config);
        NamedExpression[] namedExpressions=nes.toArray(new NamedExpression[nes.size()]);
        Project project=new Project(namedExpressions);
        return project;
    }

    public static List<NamedExpression> getProjectionEntry(Scan scan,DrillConfig config){
        //JsonNode projectionNode=scan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_PROJECTIONS);
        List<Map<String,Object>> selectionList=getSelectionMap(scan);
        List<NamedExpression> nes=new ArrayList<>();
        for(Map<String,Object> selecitonMap: selectionList){
            List<Map<String,Object>> projections=(List<Map<String,Object>>)
                    selecitonMap.get(SELECTION_KEY_WORD_PROJECTIONS);
            for(Map<String,Object> projection: projections){
                FieldReference ref=new FieldReference((String)projection.get("ref"),null);
                LogicalExpression le=new SchemaPath((String)projection.get("expr"),null);
                NamedExpression ne=new NamedExpression(le,ref);
                nes.add(ne);
            }
        }
        return nes;
    }

    public static List<LogicalOperator> getParents(LogicalOperator scan, LogicalPlan plan){
        AdjacencyList<LogicalOperator> child2Parents = plan.getGraph().getAdjList().getReversedList();
        Collection<AdjacencyList<LogicalOperator>.Node> leaves = child2Parents.getInternalRootNodes();
        AdjacencyList<LogicalOperator>.Node child=null;
        List<LogicalOperator> parents=new ArrayList<>();
        for(AdjacencyList<LogicalOperator>.Node leaf: leaves){
            if(leaf.getNodeValue().equals(scan))
            {
                child=leaf;
                break;
            }
        }
        List<Edge<AdjacencyList<LogicalOperator>.Node>> parentEdges = child2Parents.getAdjacent(child);
        for(Edge<AdjacencyList<LogicalOperator>.Node> edge: parentEdges){
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

    public static LogicalPlanUtil.RowKeyRange getRowKeyRange(Scan scan){
        JsonNode selection=scan.getSelection().getRoot().get(0);
        JsonNode rowKey=selection.get(Selections.SELECTION_KEY_WORD_ROWKEY);
        String startRowKey=rowKey.get(Selections.SELECTION_KEY_WORD_ROWKEY_START).textValue();
        String endRowKey=rowKey.get(Selections.SELECTION_KEY_WORD_ROWKEY_END).textValue();
        return new LogicalPlanUtil.RowKeyRange(startRowKey,endRowKey);
    }



    public boolean isRowKeyCrossed(LogicalPlanUtil.RowKeyRange range1, LogicalPlanUtil.RowKeyRange range2){
        if(Bytes.compareTo(range1.getStartRowKey(),range2.getEndRowKey())>0 ||
                Bytes.compareTo(range1.getEndRowKey(),range2.getStartRowKey())<0)
            return false;
        return true;
    }
    private boolean isScanCrossed(ScanWithPlan swp1,ScanWithPlan swp2){
        RowKeyRange range1=getRowKeyRange(swp1.scan);
        RowKeyRange range2=getRowKeyRange(swp2.scan);
        return isRowKeyCrossed(range1, range2);
    }

    public static  boolean isRkRangeInScan(RowKeyRange range, ScanWithPlan swp) {
        RowKeyRange scanRange=LogicalPlanUtil.getRowKeyRange(swp.scan);
        byte[] scanSrk=scanRange.getStartRowKey(),scanEnk=scanRange.getEndRowKey();
        byte[] srk=range.getStartRowKey(),enk=range.getEndRowKey();
        if(Bytes.compareTo(srk,scanSrk)>=0&&Bytes.compareTo(enk,scanEnk)<=0)
            return true;
        return false;
    }
    public static boolean isRangeEquals(RowKeyRange range1,RowKeyRange range2){
        if(Bytes.compareTo(range1.getStartRowKey(),range2.getStartRowKey())==0&&
                Bytes.compareTo(range1.getEndRowKey(),range2.getEndRowKey())==0)
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

    public static  Mergeability<Scan> equals(Scan scan1, Scan scan2) throws Exception {
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
          String storageEngine = scan.getStorageEngine();
          return scan.getSelection().getRoot().get(0).get("table").asText();
        } catch (Exception e) {
          throw new Exception(e);
        }
    }

    public static enum MergeType {
        same, belongsto,sametable
    }

    public static class RowKeyRange{
        byte[] startRowKey,endRowKey;
        public RowKeyRange(String srk,String enk){
            this.startRowKey= ByteUtils.toBytesBinary(srk);
            this.endRowKey=ByteUtils.toBytesBinary(enk);
        }
        public byte[] getStartRowKey(){
            return startRowKey;
        }
        public byte[] getEndRowKey(){
            return endRowKey;
        }
        public String toString(){
            return "srk: "+ByteUtils.toStringBinary(startRowKey)+"\n"
                    +"enk: "+ByteUtils.toStringBinary(endRowKey);
        }
    }

    public static class RowKeyRangeComparator implements Comparator<RowKeyRange> {
        @Override
        public int compare(RowKeyRange o1, RowKeyRange o2) {
            return Bytes.compareTo(o1.getStartRowKey(), o2.getStartRowKey());
        }
    }

    public static class ScanRkCompartor implements Comparator<ScanWithPlan>{

        @Override
        public int compare(ScanWithPlan o1, ScanWithPlan o2) {

            RowKeyRange range1=getRowKeyRange(o1.scan);
            RowKeyRange range2=getRowKeyRange(o2.scan);
            return Bytes.compareTo(range1.getStartRowKey(),range2.getStartRowKey());

        }
    }


  public static void addUidRangeInfo(LogicalPlan mergedPlanJson, int startBucketPos, int offsetBucketLen) throws IOException {
    DrillConfig config = DrillConfig.create();
    Pair uidRange = getLocalSEUidOfBucket(startBucketPos, offsetBucketLen);
    Collection<SourceOperator> leaves = mergedPlanJson.getGraph().getLeaves();
    for (SourceOperator leaf : leaves) {
      if (leaf instanceof Scan) {
        JSONOptions selections = ((Scan) leaf).getSelection();
        ObjectMapper mapper = config.getMapper();
        List<Map<String, Object>> selectionsModel = new ArrayList<>();
        for(JsonNode selection : selections.getRoot()) {
          Map<String, Object> map = new HashMap<>(4);
          if (((Scan) leaf).getStorageEngine().equals(QueryMasterConstant.STORAGE_ENGINE.hbase.name())) {
            Map<String, Object> rowkeyRangeMap = new HashMap<>(2);
            rowkeyRangeMap.put(SELECTION_KEY_WORD_ROWKEY_START,
                    selection.get(SELECTION_KEY_WORD_ROWKEY).get(SELECTION_KEY_WORD_ROWKEY_START));
            rowkeyRangeMap.put(SELECTION_KEY_WORD_ROWKEY_END,
                    selection.get(SELECTION_KEY_WORD_ROWKEY).get(SELECTION_KEY_WORD_ROWKEY_END));
            map.put(SELECTION_KEY_WORD_ROWKEY, rowkeyRangeMap);
            map.put(SELECTION_KEY_WORD_FILTERS, selection.get(SELECTION_KEY_WORD_FILTERS));
            //增加uid range信息
            Map<String, Object> uidRangeMap = new HashMap<>(2);
            uidRangeMap.put(SELECTION_KEY_UID_RANGE_START, uidRange.getK());
            uidRangeMap.put(SELECTION_KEY_UID_RANGE_END, uidRange.getV());
            map.put(SELECTION_KEY_UID_RANGE, uidRangeMap);

          } else if (((Scan) leaf).getStorageEngine().equals(QueryMasterConstant.STORAGE_ENGINE.mysql.name())) {
            //Mysql把uid range信息加入到filter里
            JsonNode filter = selection.get(SELECTION_KEY_WORD_FILTER);
            String uidRangeStr = "uid>=" + uidRange.getK() + " and uid<" + uidRange.getV();
            if (filter != null) {
              String filterStr = filter.asText() + " and " + uidRangeStr;
              map.put(SELECTION_KEY_WORD_FILTER, filterStr);
            } else {
              map.put(SELECTION_KEY_WORD_FILTER, uidRangeStr);
            }

          }
          map.put(SELECTION_KEY_WORD_TABLE, selection.get(SELECTION_KEY_WORD_TABLE));
          map.put(SELECTION_KEY_WORD_PROJECTIONS, selection.get(SELECTION_KEY_WORD_PROJECTIONS));
          selectionsModel.add(map);
        }
        String s = mapper.writeValueAsString(selectionsModel);
        JSONOptions selectionWithUidRange = mapper.readValue(s, JSONOptions.class);
        ((Scan) leaf).setSelection(selectionWithUidRange);
      }
    }
  }

  public static Pair getLocalSEUidOfBucket(int startBucketPos, int offsetBucketLen) {
    long startBucket = startBucketPos;
    startBucket = startBucket << 32;
    long endBucket = 0;
    if (startBucket + offsetBucketLen >= 255) {
      endBucket = (1l << 40) - 1l;
    } else {
      endBucket = startBucketPos + offsetBucketLen;
      endBucket = endBucket << 32;
    }

    return new Pair(startBucket, endBucket);
  }




}
