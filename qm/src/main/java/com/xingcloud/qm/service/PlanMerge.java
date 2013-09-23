package com.xingcloud.qm.service;

import static org.apache.drill.common.util.Selections.*;


import com.xingcloud.qm.utils.GraphVisualize;
import com.xingcloud.qm.utils.LogicalPlanUtil.*;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.xingcloud.meta.ByteUtils;
import com.xingcloud.qm.utils.LogicalPlanUtil;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.graph.AdjacencyList;
import org.apache.drill.common.graph.Edge;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.jgrapht.DirectedGraph;
import org.jgrapht.UndirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.graph.SimpleGraph;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.text.SimpleDateFormat;
import java.util.*;

public class PlanMerge {

  private final List<LogicalPlan> incoming;

  private List<LogicalPlan> splitedPlans;

  private Map<LogicalPlan, LogicalPlan> splitedPlan2Orig;

  private Map<LogicalPlan, LogicalPlan> merged;

  private Map<String, List<LogicalPlan>> sortedByProjectID;

  public static Logger logger= LoggerFactory.getLogger(PlanMerge.class);

  public PlanMerge(List<LogicalPlan> plans) throws Exception {
    this.incoming = plans;
  }

  /**
   * merge scan after slicing scan according to the row key range
   * @param plans
   * @param config
   * @return
   * @throws Exception
   */
  private Map<LogicalPlan,LogicalPlan> sortAndMergePlans(List<LogicalPlan> plans,DrillConfig config) throws Exception {
        Map<LogicalPlan,LogicalPlan> resultMap=new HashMap<>();
        /**
         * sort into sortedByProjectID, by projectID
         */
        sortPlanByProjectId(plans);
        Map<String,ProjectMergeContext> projectCtxMap=splitByTableName();
        markMergability(projectCtxMap,MergeType.same);
        resultMap=mergePlans(projectCtxMap);
        return  resultMap;
  }

  Comparator<ScanWithPlan> swpComparator = new Comparator<ScanWithPlan>() {
      @Override
      public int compare(ScanWithPlan o1, ScanWithPlan o2) {
          RowKeyRange range1=LogicalPlanUtil.getRowKeyRange(o1.scan);
          RowKeyRange range2=LogicalPlanUtil.getRowKeyRange(o2.scan);
          return Bytes.compareTo(range1.getStartRowKey(), range2.getStartRowKey());
      }
  };
  
  /**
   * Collect all the scans that belong to the same project to get a global row key range list, 
   * then slice each scan into one or more smaller scans according to the row key range list. 
   * @param origPlans the logical plan that original scan has been sliced by its selections(each day a selection)
   * @param config
   * @return plan with the most smallest granularity Scan
   * @throws Exception
   */
  private Map<LogicalPlan,LogicalPlan> splitScanByRowKey(List<LogicalPlan> origPlans,DrillConfig config) throws Exception {
      sortPlanByProjectId(origPlans); //sortedByProjectID: projectID => set<LogicalPlan>
      Map<String,ProjectMergeContext> projectCtxMap=splitByTableName();
      List<LogicalPlan> resultPlans=new ArrayList<>();
      Map<LogicalPlan,LogicalPlan> resultPlanMap=new HashMap<>();
      for(Map.Entry<String,ProjectMergeContext> entry: projectCtxMap.entrySet()){
          String projectId=entry.getKey();
          ProjectMergeContext ctx=entry.getValue();
          Map<String,Set<ScanWithPlan>> tableName2Scans=ctx.tableName2Plans;
          Map<ScanWithPlan, List<LogicalOperator>> scan2Los=new HashMap<>();
          for(Map.Entry<String,Set<ScanWithPlan>> entry1: tableName2Scans.entrySet()){
              String tableName=entry1.getKey();
              if(!tableName.contains("deu"))continue;
              ScanWithPlan[] swps=new ArrayList<>(entry1.getValue()).toArray
                      (new ScanWithPlan[entry1.getValue().size()]);
              Arrays.sort(swps, swpComparator);
              String[] rkPoints=new String[swps.length*2];
              Map<RowKeyRange,List<ScanWithPlan>> crosses=new HashMap<>();
              Map<ScanWithPlan,List<RowKeyRange>> scanSplits=new HashMap<>();
            
              // construct the rowkey range
              for(int i=0;i<swps.length;i++){
                 RowKeyRange range=LogicalPlanUtil.getRowKeyRange(swps[i].scan);
                 rkPoints[i*2]=ByteUtils.toStringBinary(range.getStartRowKey());
                 rkPoints[i*2+1]=ByteUtils.toStringBinary(range.getEndRowKey());
              }
              Arrays.sort(rkPoints,new Comparator<String>() {
                  @Override
                  public int compare(String o1, String o2) {
                      if(o1==null||o2==null)
                          System.out.println("null!!!!!!!");
                      byte[] rk1=ByteUtils.toBytesBinary(o1);
                      byte[] rk2=ByteUtils.toBytesBinary(o2);
                      return Bytes.compareTo(rk1,rk2);
                  }
              });
            
              // pick row key ranges for every scan and reverse
              for(int i=0;i<rkPoints.length-1;i++){
                  RowKeyRange range=new RowKeyRange(rkPoints[i],rkPoints[i+1]);
                  for(int j=0;j<swps.length;j++){
                      if(LogicalPlanUtil.isRkRangeInScan(range, swps[j])){
                         List<ScanWithPlan> swpList=crosses.get(range);
                         if(null==swpList){
                             swpList=new ArrayList<>();
                             crosses.put(range,swpList);
                         }
                         swpList.add(swps[j]);
                         List<RowKeyRange> rangeList=scanSplits.get(swps[j]);
                         if(rangeList==null){
                             rangeList=new ArrayList<>();
                             scanSplits.put(swps[j],rangeList);
                         }
                         rangeList.add(range);
                      }
                  }
              }

              //crosses: range => scans; scanSplits: scan => ranges
              //slice scan to union(smaller scans)
              for(Map.Entry<ScanWithPlan,List<RowKeyRange>> entry2: scanSplits.entrySet()){
                  ScanWithPlan swp=entry2.getKey();
                  List<RowKeyRange> rangeList=entry2.getValue();
                  if(rangeList.size()==1&&crosses.get(rangeList.get(0)).size()==1)
                      continue;
                  List<LogicalOperator> unionInputs=new ArrayList<>();
                  List<LogicalOperator> operators=new ArrayList<>();
                  // get union--->List(project-->filter--->scan)
                  for(RowKeyRange range: rangeList){
                      List<ScanWithPlan> swpList=crosses.get(range);
                      Scan baseScan= LogicalPlanUtil.getBaseScan(range, swpList, config); // base scan: each row key range => base scan, todo: we only need to construct once for each range
                      if(swpList.size()==1){
                          unionInputs.add(baseScan);
                          operators.add(baseScan);
                      }
                      else{
                          operators.add(baseScan);

                          Filter filter= LogicalPlanUtil.getFilter(swp.scan,config);
                          if(filter!=null){
                            filter.setInput(baseScan);

                            if(LogicalPlanUtil.getProjectionEntry(swp.scan,config).size()<
                                LogicalPlanUtil.getProjectionEntry(baseScan,config).size()){
                                Project project=LogicalPlanUtil.getProject(swp.scan,config);
                                project.setInput(filter);
                                operators.add(filter);
                                operators.add(project);
                                unionInputs.add(project);
                            }else {
                                operators.add(filter);
                                unionInputs.add(filter);
                            }

                          }else{
                              if(LogicalPlanUtil.getProjectionEntry(swp.scan,config).size()<
                                      LogicalPlanUtil.getProjectionEntry(baseScan,config).size()){
                                  Project project=LogicalPlanUtil.getProject(swp.scan,config);
                                  project.setInput(baseScan);
                                  //operators.add(filter);
                                  operators.add(project);
                                  unionInputs.add(project);
                              }else {
                                unionInputs.add(baseScan);
                              }
                          }
                      }
                  }
                  if(unionInputs.size()!=1){
                      Union union=new Union(unionInputs.toArray(new LogicalOperator[unionInputs.size()]),false);
                      operators.add(union);

                  }

                  scan2Los.put(swp,operators);
              }
          }
          
          //substitude the big scan with the union
          for(LogicalPlan plan: sortedByProjectID.get(projectId)){
              List<LogicalOperator> operators=plan.getSortedOperators();
              Set<ScanWithPlan> swps=ctx.plan2Swps.get(plan);
              for(ScanWithPlan swp:swps){
                  String tableName=LogicalPlanUtil.getTableName(swp.scan);
                  if(!tableName.contains("deu"))continue;
                  List<LogicalOperator> targetOperators=scan2Los.get(swp);
                  if(targetOperators!=null)
                  {
                     for(LogicalOperator lo: LogicalPlanUtil.getParents(swp.scan, plan)){
                         LogicalPlanUtil.substituteInParent(swp.scan,
                                 targetOperators.get(targetOperators.size() - 1), lo); // get the union

                     }
                     operators.addAll(targetOperators);
                     operators.remove(swp.scan);
                 }

              }
              PlanProperties head=plan.getProperties();
              Map<String,StorageEngineConfig> storageEngines=plan.getStorageEngines();
              LogicalPlan resultPlan=new LogicalPlan(head,storageEngines,operators);
              resultPlans.add(resultPlan);
              resultPlanMap.put(plan,resultPlan);
          }
      }
      return resultPlanMap;
  }

  /**
   * merge all the scans that belong to the same project to a UnionedScan
   * @param plans
   * @param config
   * @return
   * @throws Exception
   */
    private Map<LogicalPlan,LogicalPlan> mergeToBigScan(List<LogicalPlan> plans,DrillConfig config) throws Exception {
        Map<LogicalPlan,LogicalPlan> result=new HashMap<LogicalPlan,LogicalPlan>();
        sortPlanByProjectId(plans);
        Map<String,ProjectMergeContext> projectCtxMap=splitByTableName();
        markMergability(projectCtxMap,MergeType.sametable);
        result=producePlansWithUniondScan(projectCtxMap,config);
        return result;
    }

  /**
   * merge all the scans that belong to the same project to a UnionedScan
   * @param projectCtxMap
   * @param config
   * @return plans with UnionedScan
   * @throws IOException
   */
  private Map<LogicalPlan,LogicalPlan> producePlansWithUniondScan(Map<String,ProjectMergeContext> projectCtxMap,DrillConfig config) throws IOException {
      Map<LogicalPlan,LogicalPlan> result=new HashMap<>();
      for (Map.Entry<String, List<LogicalPlan>> entry : sortedByProjectID.entrySet()) {
          String project=entry.getKey();
          ProjectMergeContext ctx=projectCtxMap.get(project);
          //开始合并
          ctx.planInspector = new ConnectivityInspector<LogicalPlan, DefaultEdge>(ctx.mergePlanSets);
          List<Set<LogicalPlan>> mergeSets = ctx.planInspector.connectedSets();
          ctx.scanInspector = new ConnectivityInspector<Scan, DefaultEdge>(ctx.mergedScanSets);
          ctx.devidedScanSets = ctx.scanInspector.connectedSets();

          for (int i = 0; i < mergeSets.size(); i++) {
              Set<LogicalPlan> planSet = mergeSets.get(i);
              LogicalPlan mergedPlan = produceBigScan(planSet, ctx,config);
              for (LogicalPlan original : planSet) {
                  //LogicalPlan realOrig = splitedPlan2Orig.get(original);
                  result.put(original, mergedPlan);
              }
          }
          ctx.close();
      }
      return result;
  }

  /**
   * produce UnionedScan for one project
   * @param plans
   * @param projectCtx
   * @param config
   * @return
   * @throws IOException
   */
  private LogicalPlan produceBigScan(Set<LogicalPlan> plans,ProjectMergeContext projectCtx,DrillConfig config) throws IOException {

      List<LogicalOperator> roots=new ArrayList<>();
      for(LogicalPlan plan: plans){
          Collection<SinkOperator> unitRoots=plan.getGraph().getRoots();
          roots.addAll(unitRoots);
      }
      //PlanMergeContext planCtx = new PlanMergeContext();
      List<LogicalOperator> operators=new ArrayList<>();
      Map<Scan,UnionedScanSplit> substituteMap=new HashMap<>();
      for(Map.Entry<String,Set<ScanWithPlan>> entry: projectCtx.tableName2Plans.entrySet()){
          //String tableName=entry.getKey();

          Set<ScanWithPlan> swps=entry.getValue();
          if(swps.size()<=1)
              continue;
          ScanWithPlan[] swpArr=new ArrayList<ScanWithPlan>(swps).toArray(new ScanWithPlan[swps.size()]);
          if(!plans.contains(swpArr[0].plan))
              continue;
          if(!swpArr[0].scan.getStorageEngine().contains("hbase"))
              continue;
          ObjectMapper mapper=config.getMapper();
          ArrayNode jsonArray=new ArrayNode(mapper.getNodeFactory());
          //List<Map<String,Object>> mapList=new ArrayList<Map<String,Object>>();
          for(int i=0;i<swpArr.length;i++){
              for(JsonNode node:swpArr[i].scan.getSelection().getRoot()){
                  jsonArray.add(node);
              }
          }
          String selectionStr=mapper.writeValueAsString(jsonArray);
          JSONOptions selection=mapper.readValue(selectionStr,JSONOptions.class);
          UnionedScan unionedScan=new UnionedScan(swpArr[0].scan.getStorageEngine(),selection,swpArr[0].scan.getOutputReference());
          List<String> memos = new ArrayList<>();

          // substitude the scan with the UnionedScanSplit
          for(int i=0;i<swpArr.length;i++){
            int[] entries=new int[1];
            entries[0]=i;
            ScanWithPlan swp = swpArr[i];
            if(swp.scan.getMemo() != null){
              memos.add(swp.scan.getMemo());
            }else{
              memos.add("n/a");
            }
            UnionedScanSplit unionedScanSplit = new UnionedScanSplit(entries);
            unionedScanSplit.setMemo(Arrays.toString(entries));
            
            unionedScanSplit.setInput(unionedScan);
            LogicalPlan plan=swpArr[i].plan;
            AdjacencyList<LogicalOperator> adjacencyList=plan.getGraph().getAdjList().getReversedList();
            for(AdjacencyList<LogicalOperator>.Node lo:adjacencyList.getInternalRootNodes()){
                if(lo.getNodeValue().equals(swpArr[i].scan)){
                    for(Edge<AdjacencyList<LogicalOperator>.Node> edge:adjacencyList.getAdjacent(lo)){
                        AdjacencyList<LogicalOperator>.Node parent=edge.getTo();
                        LogicalOperator parentLo=parent.getNodeValue();
                        LogicalPlanUtil.substituteInParent(swpArr[i].scan,unionedScanSplit,parentLo);

                    }
                }
            }

            substituteMap.put(swpArr[i].scan,unionedScanSplit);
          }
          unionedScan.setMemo(memos.toString());  

          operators.add(unionedScan);
          /*
          for(int i=0;i<swpArr.length;i++){
              List<LogicalOperator> origPlanOperators=swpArr[i].plan.getSortedOperators();
              for(LogicalOperator lo: origPlanOperators){
                  LogicalOperator target=substituteMap.get(lo);
                  if(target!=null){
                      operators.add(target);
                  }else{
                      if(operators.contains(lo))operators.add(lo);
                  }
              }
          }
          */
      }
      PlanProperties head = null;
      Map<String, StorageEngineConfig> se = null;
      for(LogicalPlan plan: plans){
          if(head==null)
              head=plan.getProperties();
          if(se==null)
              se=plan.getStorageEngines();
          List<LogicalOperator> origPlanOperators=plan.getSortedOperators();
          for(LogicalOperator lo: origPlanOperators){
              if(substituteMap.containsKey(lo)){
                 LogicalOperator target=substituteMap.get(lo);
                 if(!operators.contains(target))
                    operators.add(substituteMap.get(lo));
              }else{
                  if(!operators.contains(lo))
                  operators.add(lo);
              }
          }
      }

      if(roots.size()>1){
         List<LogicalOperator> unionInputs=new ArrayList<>();
         //Store store=null;
         for(LogicalOperator root: roots){
             if(root instanceof Store){
                 unionInputs.add(((Store)root).getInput());
                 ((Store)root).setInput(null);
                 operators.remove(root);
             }else {
                 unionInputs.add(root);
             }
         }
         Union union=new Union(unionInputs.toArray(new LogicalOperator[unionInputs.size()]),false);
         Store store=new Store(((Store)roots.get(0)).getStorageEngine(),((Store)roots.get(0)).getTarget(),((Store)roots.get(0)).getPartition());
         store.setInput(union);
         operators.add(union);operators.add(store);
      }

      return new LogicalPlan(head, se, operators);
  }

  /*

    public JSONOptions toSingleJsonOptions() throws IOException {
        List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>(1);
        mapList.add(this.toSelectionMap());
        ObjectMapper mapper = config.getMapper();
        String s = mapper.writeValueAsString(mapList);
        return mapper.readValue(s, JSONOptions.class);
    }

  */

  /**
   * Slice the scan that has multi projections into one or more smaller scans, one smaller scan corresponding to one projection.
   * The projections of a scan were generated by qm according to the original star/end row key and events that related.   
   * @param plans
   * @param config
   * @return original logicplan => tranlated logicalplan
   * @throws IOException
   */
    public Map<LogicalPlan,LogicalPlan> splitBigScan(List<LogicalPlan> plans,DrillConfig config) throws IOException {
    splitedPlans = new ArrayList<>();
    splitedPlan2Orig = new HashMap<>();
    Map<LogicalPlan,LogicalPlan> results=new HashMap<>();
    int index = 0;
    for (LogicalPlan plan : plans) {
      AdjacencyList<LogicalOperator> child2Parents = plan.getGraph().getAdjList().getReversedList();
      Collection<AdjacencyList<LogicalOperator>.Node> leaves = child2Parents.getInternalRootNodes();
      List<LogicalOperator> operators = new ArrayList<>();
      Map<AdjacencyList<LogicalOperator>.Node, Union> scanNodeUnionMap = new HashMap<>();
      List<AdjacencyList<LogicalOperator>.Node> nextStep = new ArrayList<>();
      for (AdjacencyList<LogicalOperator>.Node leaf : leaves) {
        if (leaf.getNodeValue() instanceof Scan) {
          Scan origScan = (Scan) leaf.getNodeValue();
          List<Scan> childScans = new ArrayList<>();
          if (origScan.getStorageEngine().contains("hbase")) {
            for (JsonNode selection : (origScan.getSelection().getRoot())) {
              Map<String, Object> map = new HashMap<String, Object>(4);
              map.put(SELECTION_KEY_WORD_TABLE, selection.get(SELECTION_KEY_WORD_TABLE));

              Map<String, Object> rowkeyRangeMap = new HashMap<String, Object>(2);
              rowkeyRangeMap.put(SELECTION_KEY_WORD_ROWKEY_START,
                                 selection.get(SELECTION_KEY_WORD_ROWKEY).get(SELECTION_KEY_WORD_ROWKEY_START));
              rowkeyRangeMap.put(SELECTION_KEY_WORD_ROWKEY_END,
                                 selection.get(SELECTION_KEY_WORD_ROWKEY).get(SELECTION_KEY_WORD_ROWKEY_END));
              map.put(SELECTION_KEY_WORD_ROWKEY, rowkeyRangeMap);
              map.put(SELECTION_KEY_WORD_PROJECTIONS, selection.get(SELECTION_KEY_WORD_PROJECTIONS));
              map.put(SELECTION_KEY_WORD_FILTERS, selection.get(SELECTION_KEY_WORD_FILTERS));

              List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>(1);
              mapList.add(map);
              ObjectMapper mapper = config.getMapper();
              String s = mapper.writeValueAsString(mapList);
              JSONOptions childSelection = mapper.readValue(s, JSONOptions.class);
              Scan childScan = new Scan(origScan.getStorageEngine(), childSelection, origScan.getOutputReference());
              childScan.setMemo(origScan.getMemo());
              childScans.add(childScan);
            }
          } else {
            operators.add(origScan);
            List<AdjacencyList<LogicalOperator>.Node> parents = getParents(leaf, child2Parents);
            for (AdjacencyList<LogicalOperator>.Node parent : parents) {
              if (!nextStep.contains(parent))
                nextStep.add(parent);
            }
          }
          
          // construct the union
          LogicalOperator[] splitedScans = new LogicalOperator[childScans.size()];
          int i = 0;
          for (Scan scan : childScans) {
            splitedScans[i++] = scan;
          }
          if (childScans.size() > 1) {
            for (int j = 0; j < splitedScans.length; j++)
              operators.add(splitedScans[j]);
            Union union = new Union(splitedScans, false);
            scanNodeUnionMap.put(leaf, union);
          } else {
            operators.add(origScan);
            List<AdjacencyList<LogicalOperator>.Node> parents = getParents(leaf, child2Parents);
            for (AdjacencyList<LogicalOperator>.Node parent : parents) {
              if (!nextStep.contains(parent))
                nextStep.add(parent);
            }
          }
        }
      }

      //substitude children to union
      for (Map.Entry<AdjacencyList<LogicalOperator>.Node, Union> entry : scanNodeUnionMap.entrySet()) {
        Union union = entry.getValue();
        AdjacencyList<LogicalOperator>.Node leaf = entry.getKey();
        LogicalOperator origOp = leaf.getNodeValue();
        List<Edge<AdjacencyList<LogicalOperator>.Node>> parentEdges = child2Parents.getAdjacent(leaf);
        for (Edge<AdjacencyList<LogicalOperator>.Node> parentEdge : parentEdges) {
          //looking for all parents of scan,
          // substitute scan with targetScan
          AdjacencyList<LogicalOperator>.Node parentNode = parentEdge.getTo();
          LogicalOperator parent = parentNode.getNodeValue();
          if (parent instanceof SingleInputOperator) {
            ((SingleInputOperator) parent).setInput(union);
          } else if (parent instanceof Join) {
            if (((Join) parent).getLeft().equals(origOp))
              ((Join) parent).setLeft(union);
            else
              ((Join) parent).setRight(union);
          } else if (parent instanceof Union) {
            for (LogicalOperator op : ((Union) parent).getInputs()) {
              if (op.equals(origOp))
                op = union;
            }
          }
          if (!nextStep.contains(parentNode))
            nextStep.add(parentNode);
        }
        operators.add(union);

      }

      putParentsToGraph(nextStep, child2Parents, operators);

      PlanProperties head = plan.getProperties();
      Map<String, StorageEngineConfig> se = plan.getStorageEngines();
      index++;
      try {
        LogicalPlan subsPlan = new LogicalPlan(head, se, operators);
        splitedPlans.add(subsPlan);
        splitedPlan2Orig.put(subsPlan, plan);
        results.put(plan,subsPlan);
      } catch (Exception e) {
        System.out.println(index + " plan has problem");
        e.printStackTrace();
      }
    }
    return results;
  }

  private List<AdjacencyList<LogicalOperator>.Node> getParents(AdjacencyList<LogicalOperator>.Node opNode,
                                                               AdjacencyList<LogicalOperator> child2Parents) {
    List<AdjacencyList<LogicalOperator>.Node> parents = new ArrayList<>();
    List<Edge<AdjacencyList<LogicalOperator>.Node>> parentEdges = child2Parents.getAdjacent(opNode);
    for (Edge<AdjacencyList<LogicalOperator>.Node> parentEdge : parentEdges) {
      AdjacencyList.Node parentNode = parentEdge.getTo();
      //LogicalOperator parent= (LogicalOperator) parentNode.getNodeValue();
      parents.add(parentNode);
    }
    return parents;
  }

  private void putParentsToGraph(List<AdjacencyList<LogicalOperator>.Node> currentOps,
                                 AdjacencyList<LogicalOperator> child2Parents, List<LogicalOperator> operators) {
    List<AdjacencyList<LogicalOperator>.Node> nextStep = new ArrayList<>();
    for (AdjacencyList<LogicalOperator>.Node opNode : currentOps) {
      LogicalOperator op = opNode.getNodeValue();
      if (!operators.contains(op))
        operators.add(op);
      List<Edge<AdjacencyList<LogicalOperator>.Node>> parentEdges = child2Parents.getAdjacent(opNode);
      for (Edge<AdjacencyList<LogicalOperator>.Node> parentEdge : parentEdges) {
        AdjacencyList.Node parentNode = parentEdge.getTo();
        LogicalOperator parent = (LogicalOperator) parentNode.getNodeValue();
        if (!operators.contains(parent) && !nextStep.contains(parentNode))
          nextStep.add(parentNode);
      }
    }
    if (nextStep.size() == 0)
      return;
    putParentsToGraph(nextStep, child2Parents, operators);

  }


  /**
   * sort the plans by project
   * @param plans
   */
  private void sortPlanByProjectId(List<LogicalPlan> plans){
      sortedByProjectID =new HashMap<String,List<LogicalPlan>>();
      for (LogicalPlan plan : plans) {
          String projectID = getProjectID(plan);
          List<LogicalPlan> projectPlans = sortedByProjectID.get(projectID);
          if (projectPlans == null) {
              projectPlans = new ArrayList<>();
              sortedByProjectID.put(projectID, projectPlans);
          }
          projectPlans.add(plan);
      }
  }

  /**
   * 
   * @return map: projectID => ProjectMergeContext
   * @throws Exception
   */
  private Map<String,ProjectMergeContext> splitByTableName() throws Exception {
      Map<String,ProjectMergeContext> projectCtxMap=new HashMap<String,ProjectMergeContext>();
      for (Map.Entry<String, List<LogicalPlan>> entry : sortedByProjectID.entrySet()) {
          String project = entry.getKey();
          List<LogicalPlan> plans = entry.getValue();

          ProjectMergeContext ctx = new ProjectMergeContext();
          projectCtxMap.put(project,ctx);
          for (LogicalPlan plan : plans) {
          //init merging plan sets:初始状态每个plan自成一组
          ctx.mergePlanSets.addVertex(plan);
          //找到有对相同table操作的scan
          Collection<SourceOperator> leaves = plan.getGraph().getLeaves();
          for (SourceOperator leaf : leaves) {
              if (leaf instanceof Scan) {
                  Scan scan = (Scan) leaf;
                  //初始状态，每个scan自成一组
                  ctx.mergedScanSets.addVertex(scan);
                  String tableName = null;
                  try {
                      tableName = LogicalPlanUtil.getTableName(scan);
                  } catch (Exception e) {
                      e.printStackTrace();
                      throw e;
                  }
                  ScanWithPlan swp = new ScanWithPlan(scan, plan, tableName);
                  Set<ScanWithPlan> swps = ctx.tableName2Plans.get(tableName);
                  if (swps == null) {
                      swps = new HashSet<>();
                      ctx.tableName2Plans.put(tableName, swps);
                  }
                  swps.add(swp);
                  Set<ScanWithPlan> scanWithPlans=ctx.plan2Swps.get(plan);
                  if(scanWithPlans==null){
                      scanWithPlans=new HashSet<>();
                      ctx.plan2Swps.put(plan,scanWithPlans);
                  }
                  scanWithPlans.add(swp);

                  Set<String> tableNames = ctx.plan2TableNames.get(plan);
                  if (tableNames == null) {
                      tableNames = new HashSet<>();
                      ctx.plan2TableNames.put(plan, tableNames);
                  }
                  tableNames.add(tableName);
              }
          }
      }
    }
      return projectCtxMap;
  }

  /**
   * 
   * @param projectCtxMap
   * @param option
   * @return
   */
  private  Map<String,ProjectMergeContext> markMergability(
          Map<String,ProjectMergeContext> projectCtxMap,MergeType option){
      Map<String,ProjectMergeContext> result=new HashMap<>();
      for (Map.Entry<String, List<LogicalPlan>> entry : sortedByProjectID.entrySet()) {
          String project=entry.getKey();
          ProjectMergeContext ctx=projectCtxMap.get(project);
          result.put(project,ctx);
          //检查scan，标记合并
          for (Map.Entry<String, Set<ScanWithPlan>> entry2 : ctx.tableName2Plans.entrySet()) {
              String tableName = entry2.getKey();
              ScanWithPlan[] swps = new ScanWithPlan[entry2.getValue().size()];
              entry2.getValue().toArray(swps);
              for (int i = 0; i < swps.length; i++) {
                  ScanWithPlan swpFrom = swps[i];
                  for (int j = i + 1; j < swps.length; j++) {
                      ScanWithPlan swpTo = swps[j];
                      Mergeability<Scan> mergeability=null;
                      switch (option){
                          case same:
                           mergeability= LogicalPlanUtil.mergeable(swpFrom.scan, swpTo.scan);
                          break;
                          case sametable:
                              mergeability=new LogicalPlanUtil.Mergeability<Scan>(MergeType.sametable,swpFrom.scan,swpTo.scan);
                          break;
                      }
                      if (mergeability != null) {
                          markMerge(swpFrom, swpTo, mergeability, ctx);
                      }

                  }
              }
          }
      }
      return result;
  }

  /**
   * merge scan after slicing scan according to the row key range
   * @param projectCtxMap
   * @return
   */
  private Map<LogicalPlan,LogicalPlan> mergePlans(Map<String,ProjectMergeContext> projectCtxMap){
      Map<LogicalPlan,LogicalPlan> mergeResult=new HashMap<>();
      for (Map.Entry<String, List<LogicalPlan>> entry : sortedByProjectID.entrySet()) {
          String project=entry.getKey();
          ProjectMergeContext ctx=projectCtxMap.get(project);
          //开始合并
          ctx.planInspector = new ConnectivityInspector<LogicalPlan, DefaultEdge>(ctx.mergePlanSets);
          List<Set<LogicalPlan>> mergeSets = ctx.planInspector.connectedSets(); // group the plans in this project by the possibility of merging
          ctx.scanInspector = new ConnectivityInspector<Scan, DefaultEdge>(ctx.mergedScanSets);
          ctx.devidedScanSets = ctx.scanInspector.connectedSets();

          for (int i = 0; i < mergeSets.size(); i++) {
              Set<LogicalPlan> planSet = mergeSets.get(i);
              LogicalPlan mergedPlan = doMergePlan(planSet, ctx);
              for (LogicalPlan original : planSet) {
                  //LogicalPlan realOrig = splitedPlan2Orig.get(original);
                  mergeResult.put(original, mergedPlan);
              }
          }
          ctx.close();
      }//for sortedByProjectID
      return mergeResult;
  }



  /**
   *  merge scan after slicing scan according to the row key range for the plans that have the possibility to merge
   *  合并两个plan的方法：
   * <p/>
   * 如果两个scan完全一样，就合并。 如果两个scan，其中一个的scan被另一个包含，就给他加一个filter，然后合并。 非叶子节点：如果完全一样（子节点也一样），就合并。
   */
  private LogicalPlan doMergePlan(Set<LogicalPlan> plans, ProjectMergeContext projectCtx) {
    if (plans.size() == 1) {
      //no need to run merge; should not run merge
      return plans.iterator().next();
    }
    PlanProperties head = null;
    Map<String, StorageEngineConfig> se = null;
    PlanMergeContext planCtx = new PlanMergeContext();
    for (LogicalPlan plan : plans) {
      AdjacencyList<LogicalOperator> adjList = plan.getGraph().getAdjList();
      if (se == null) {
        se = plan.getStorageEngines();
        head = plan.getProperties();
      }
      AdjacencyList<LogicalOperator> child2Parents = plan.getGraph().getAdjList().getReversedList();
      Collection<AdjacencyList<LogicalOperator>.Node> leaves = child2Parents.getInternalRootNodes();
      Set<AdjacencyList<LogicalOperator>.Node> nextStepSet = new HashSet<AdjacencyList<LogicalOperator>.Node>();
      //merge leaves first; then merge their parents
      for (AdjacencyList<LogicalOperator>.Node leaf : leaves) {
        LogicalOperator op = leaf.getNodeValue();
        if (!(op instanceof Scan)) {
          continue;
        }
        Scan scan = (Scan) op;
        if (planCtx.mergedFrom2To.containsKey(scan)) {
          //already merged
          continue;
        }
        //look for a already merged scan to which this scan can be merged
        Set<Scan> connectedScans = projectCtx.scanInspector.connectedSetOf(scan);
        Scan targetScan = null;
        if (connectedScans != null && connectedScans.size() > 1) {
          for (Scan connectedScan : connectedScans) {
            if (connectedScan == scan) {
              continue;
            }
            if (planCtx.mergedFrom2To.containsKey(connectedScan)) {
              targetScan = (Scan) planCtx.mergedFrom2To.get(connectedScan);
              break;
            }
          }
        }
        doMergeOperator(scan, targetScan, planCtx);

        lookForParentsAndSubstitute(leaf, child2Parents, nextStepSet, targetScan);

      }//for leaves
      //merge parents from leave, starting from nextStepSet
      for (; nextStepSet.size() > 0; ) {
        Set<AdjacencyList<LogicalOperator>.Node> currentStepSet = nextStepSet;
        nextStepSet = new HashSet<AdjacencyList<LogicalOperator>.Node>();
        //purge first, do merge for operators whose children have all already been merged
        for (Iterator<AdjacencyList<LogicalOperator>.Node> iterator = currentStepSet.iterator(); iterator.hasNext(); ) {
          AdjacencyList<LogicalOperator>.Node op = iterator.next();
          boolean childrenFullyMerged = true;

          for (LogicalOperator child : op.getNodeValue()) {
            if (!planCtx.mergedFrom2To.containsKey(child)) {
              childrenFullyMerged = false;
              break;
            }
          }
          if (!childrenFullyMerged) {
            iterator.remove();
          }
        }
        for (AdjacencyList<LogicalOperator>.Node opNode : currentStepSet) {
          LogicalOperator op = opNode.getNodeValue();
          // check carefully and merge
          // 遍历自己的所有儿子节点的其他父亲节点，看有没有合适的
          LogicalOperator mergeTo = null;
          childrenLoop:
          for (LogicalOperator child : op) {
            if (!planCtx.mergedGraph.containsVertex(child)) {
              //只寻找已经被合并了的儿子节点
              continue;
            }
            Set<DefaultEdge> childEdges = planCtx.mergedGraph.incomingEdgesOf(child);
            for (DefaultEdge e : childEdges) {
              LogicalOperator otherParent = planCtx.mergedGraph.getEdgeSource(e);
              if (LogicalPlanUtil.equals(op, otherParent) != null) {
                //found someone to merge
                mergeTo = otherParent;
                break childrenLoop;
              }
            }
          }
          doMergeOperator(op, mergeTo, planCtx);
          lookForParentsAndSubstitute(opNode, child2Parents, nextStepSet, mergeTo);
        }
      }
    }//for plans
    //add union/store to all roots
    List<LogicalOperator> roots = new ArrayList<LogicalOperator>();
    Set<LogicalOperator> vs = planCtx.mergedGraph.vertexSet();
    for (LogicalOperator op : vs) {
      if (planCtx.mergedGraph.inDegreeOf(op) == 0) {
        roots.add(op);
      }
    }
    if (roots.size() > 1) {
      //todo 输入的plan需要store节点来指明哪里需要输出结果。
      //这里需要把store摘除，然后连接上union
      List<LogicalOperator> unionChildren = new ArrayList<>();
      for (int i = 0; i < roots.size(); i++) {
        LogicalOperator root = roots.get(i);
        if (root instanceof Store) {
          unionChildren.add(((Store) root).getInput());
          //detach from children
          ((Store) root).setInput(null);
          planCtx.mergeResult.remove(root);
        } else {
          unionChildren.add(root);
        }
      }
      LogicalOperator[] unionChilds = new LogicalOperator[unionChildren.size()];
      for (int i = 0; i < unionChildren.size(); i++)
        unionChilds[i] = unionChildren.get(i);
      Union union = new Union(unionChilds, false);
      doMergeOperator(union, null, planCtx);
      Store store = new Store("output", null, null);
      store.setInput(union);
      doMergeOperator(store, null, planCtx);
    } else {
      if (!(roots.get(0) instanceof Store)) {
        Store store = new Store("output", null, null);
        store.setInput(roots.get(0));
        doMergeOperator(store, null, planCtx);
      }
    }
    return new LogicalPlan(head, se, planCtx.mergeResult);
  }

  private void doMergeOperator(LogicalOperator source, LogicalOperator target, PlanMergeContext planCtx) {
    if (target == null) {
      //no merge can be done; it's a new operator added to mergeResult
      planCtx.mergeResult.add(source);
      planCtx.mergedFrom2To.put(source, source);
      //new node added, change graph
      planCtx.mergedGraph.addVertex(source);
      for (LogicalOperator child : source) {
        if (planCtx.mergedGraph.containsEdge(source, child)) {
          logger.info("noooo!");
        }
        planCtx.mergedGraph.addEdge(source, child);
      }
    } else {
      //merge scan with targetScan
      planCtx.mergedFrom2To.put(source, target);
      //detach children from source
      if (source instanceof SingleInputOperator) {
        ((SingleInputOperator) source).setInput(null);
      } else if (source instanceof Join) {
        ((Join) source).setLeft(null);
        ((Join) source).setRight(null);
      } else if (source instanceof Union) {
        for (LogicalOperator op : ((Union) source).getInputs()) {
          op = null;
        }

      }
    }
  }

  private void lookForParentsAndSubstitute(AdjacencyList<LogicalOperator>.Node child,
                                           AdjacencyList<LogicalOperator> child2Parents,
                                           Collection<AdjacencyList<LogicalOperator>.Node> output,
                                           LogicalOperator substitutionInParents) {
    List<Edge<AdjacencyList<LogicalOperator>.Node>> parentEdges = child2Parents.getAdjacent(child);
    for (Edge<AdjacencyList<LogicalOperator>.Node> parentEdge : parentEdges) {
      //looking for all parents of scan, 
      // substitute scan with targetScan
      AdjacencyList<LogicalOperator>.Node parentNode = parentEdge.getTo();
      LogicalOperator parent = parentNode.getNodeValue();
      //add parent for candidate for next round check
      output.add(parentNode);
      if (substitutionInParents != null) {
        LogicalPlanUtil.substituteInParent(child.getNodeValue(), substitutionInParents, parent);
      }
    }
  }

  private void markMerge(ScanWithPlan swpFrom, ScanWithPlan swpTo, Mergeability<Scan> mergeability,
                         ProjectMergeContext ctx) {
    ctx.mergePlanSets.addVertex(swpFrom.plan);
    ctx.mergePlanSets.addVertex(swpTo.plan);
    if(swpFrom.plan!=swpTo.plan)
        ctx.mergePlanSets.addEdge(swpFrom.plan, swpTo.plan);
    ctx.mergedScanSets.addVertex(mergeability.from);
    ctx.mergedScanSets.addVertex(mergeability.to);
    if(mergeability.from!=mergeability.to)
        ctx.mergedScanSets.addEdge(mergeability.from, mergeability.to);

  }

  static class PlanMergeContext {
    //merge output
    List<LogicalOperator> mergeResult = new ArrayList<>();
    //all operators merged
    Map<LogicalOperator, LogicalOperator> mergedFrom2To = new HashMap<>();
    DirectedGraph<LogicalOperator, DefaultEdge> mergedGraph = new SimpleDirectedGraph<LogicalOperator, DefaultEdge>(
      DefaultEdge.class);

  }

  static class ProjectMergeContext {
    Map<String, Set<ScanWithPlan>> tableName2Plans = new HashMap<>();//equals
    Map<LogicalPlan, Set<String>> plan2TableNames = new HashMap<>();//equals
    Map<LogicalPlan,Set<ScanWithPlan>> plan2Swps=new HashMap<>();
    UndirectedGraph<LogicalPlan, DefaultEdge> mergePlanSets = new SimpleGraph<LogicalPlan, DefaultEdge>(
      DefaultEdge.class);//==
    UndirectedGraph<Scan, DefaultEdge> mergedScanSets = new SimpleGraph<Scan, DefaultEdge>(DefaultEdge.class);//==
    ConnectivityInspector<Scan, DefaultEdge> scanInspector = null;
    ConnectivityInspector<LogicalPlan, DefaultEdge> planInspector = null;

    List<Set<Scan>> devidedScanSets = null;

    public void close() {
      DirectedGraph<Scan, DefaultEdge> g = new SimpleDirectedGraph<>(DefaultEdge.class);

      tableName2Plans.clear();
      tableName2Plans = null;
      plan2TableNames.clear();
      plan2TableNames = null;

    }
  }

  public static class ScanWithPlan {
    public Scan scan;
    public LogicalPlan plan;
    public String tableName;

    ScanWithPlan() {
    }

    ScanWithPlan(Scan scan, LogicalPlan plan, String tableName) {
      this.scan = scan;
      this.plan = plan;
      this.tableName = tableName;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      ScanWithPlan that = (ScanWithPlan) o;

      if (!plan.equals(that.plan))
        return false;
      if (!scan.equals(that.scan))
        return false;
      if (!tableName.equals(that.tableName))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = scan.hashCode();
      result = 31 * result + plan.hashCode();
      result = 31 * result + tableName.hashCode();
      return result;
    }
  }

  /**
   * LogicalPlan 必须带上plan属于的project的信息，才能被合并。
   *
   * @param plan
   * @return
   */
  public static String getProjectID(LogicalPlan plan) {
    return plan.getProperties().generator.info;
  }

  public static void setProjectID(LogicalPlan plan, String projectID) {
    plan.getProperties().generator.info = projectID;
  }



  /**
   * @param plans 需要merge的LogicalPlan 列表
   * @return 原始的LogicalPlan和合并以后LogicalPlan之间的对应关系。 Map的key 是 原始的LogicalPlan，value是合并后的LogicalPlan。 如果输入的plans当中，有plan
   *         没有和别的plan合并，则在返回的map中，key和value都是这个plan。
   */
  public static Map<LogicalPlan, LogicalPlan> sortAndMerge(List<LogicalPlan> plans,DrillConfig config) throws Exception {
    long t=System.currentTimeMillis();
      SimpleDateFormat format=new SimpleDateFormat("ssmmhh-yyyyMMdd");
    File dir =new File("/home/hadoop/yangbo/planMerges/"+format.format(new Date(t)));
    dir.mkdir();
    File sourcefile=new File(dir.getAbsolutePath()+"/source.log");
    File targetFile=new File(dir.getAbsolutePath()+"/target.log");
    logger.info("before merge !!!!!!!!!!!");
    Writer sourcewriter=new FileWriter(sourcefile);
    sourcewriter.write("before merge !!!!!!!!!!!\n\r");
    for(LogicalPlan plan : plans){
        //logger.info("-------------------");
        sourcewriter.write("------------------\n\r");
        //logger.info(config.getMapper().writeValueAsString(plan));
        sourcewriter.write(config.getMapper().writeValueAsString(plan)+"\n\r");
    }
    sourcewriter.write("merge before !!!!!!!!!!!!\n\r");
    logger.info("merge before !!!!!!!!!!!!");
    PlanMerge planMerge = new PlanMerge(plans);
    Map<LogicalPlan,LogicalPlan> splitBigPlanMap=planMerge.splitBigScan(plans,config);
    List<LogicalPlan> bigPlanSplitedPlans=new ArrayList(splitBigPlanMap.values());
    Map<LogicalPlan,LogicalPlan> splitRkPlanMap=
            planMerge.splitScanByRowKey(bigPlanSplitedPlans,config);
    List<LogicalPlan> rkSplitedPlans=new ArrayList<>(splitRkPlanMap.values());
    int index=0;
    for(LogicalPlan pl: rkSplitedPlans){
        //GraphVisualize.visualize(pl,"test-rkSplited"+(index++)+".png");
    }
    Map<LogicalPlan,LogicalPlan> mergePlanMap=planMerge.sortAndMergePlans(rkSplitedPlans,config);
    Set<LogicalPlan> scanMergedPlanSet=new HashSet<>(mergePlanMap.values());
    List<LogicalPlan> scanMergedPlans=new ArrayList<>(scanMergedPlanSet);
    index=0;
    for(LogicalPlan pl: scanMergedPlans){

          //GraphVisualize.visualize(pl,"test-scanMerged"+(index++)+".png");
    }
    Map<LogicalPlan,LogicalPlan> mergeToTalbeScanMap=planMerge.mergeToBigScan(scanMergedPlans,config);
    Map<LogicalPlan,LogicalPlan> result=new HashMap<>();
    Writer targetWriter=new FileWriter(targetFile);

    logger.info("after merge ------------------------");
    for(Map.Entry<LogicalPlan,LogicalPlan> entry: splitBigPlanMap.entrySet()){
        LogicalPlan orig=entry.getKey();
        LogicalPlan splitBigScanResultPlan=splitBigPlanMap.get(entry.getKey());
        LogicalPlan splitRkResultPlan=splitRkPlanMap.get(splitBigScanResultPlan);
        LogicalPlan mergePlanResultPlan=mergePlanMap.get(splitRkResultPlan);
        LogicalPlan mergeToTableScanResultPlan=mergeToTalbeScanMap.get(mergePlanResultPlan);
        //logger.info(config.getMapper().writeValueAsString(mergeToTableScanResultPlan));
        targetWriter.write("--------------------------------------\n\r");
        targetWriter.write(config.getMapper().writeValueAsString(mergeToTableScanResultPlan)+"\n\r");
        result.put(orig, mergeToTableScanResultPlan);
    }
    logger.info("merge after ------------------------");
    sourcewriter.flush();
    sourcewriter.close();
    targetWriter.flush();
    targetWriter.close();
    return result;
    //return planMerge.getMerged();
  }

  public Map<LogicalPlan, LogicalPlan> getMerged() {
    return merged;
  }




}
