package com.xingcloud.qm.service;

import static org.apache.drill.common.util.Selections.*;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.xingcloud.qm.config.QMConfig;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.xingcloud.qm.utils.LogicalPlanUtil;
import com.xingcloud.qm.utils.LogicalPlanUtil.*;
import com.xingcloud.qm.utils.PlanWriter;
import com.xingcloud.qm.utils.QueryMasterConstant;
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

import java.io.IOException;
import java.util.*;


public class PlanMerge {

    private final List<LogicalPlan> incoming;

    private List<LogicalPlan> splitedPlans;

    private Map<LogicalPlan, LogicalPlan> splitedPlan2Orig;

    private Map<LogicalPlan, LogicalPlan> merged;

    private Map<String, List<LogicalPlan>> sortedByProjectID;

    public static Logger logger = LoggerFactory.getLogger(PlanMerge.class);

    public PlanMerge(List<LogicalPlan> plans) throws Exception {
        this.incoming = plans;
    }

    public static void transferPlan(List<LogicalPlan> plans, DrillConfig config) throws Exception {
      for(LogicalPlan plan : plans){
        long st = System.currentTimeMillis();
        LogicalPlanUtil.getRkRangePlan(plan, config);
        logger.debug("transfer one plan using " + (System.currentTimeMillis() - st) + " ms");
      }
    }

  /**
   * merge scan after slicing scan according to the row key range
   * @param plans
   * @param config
   * @return
   * @throws Exception
   */
  private Map<LogicalPlan, LogicalPlan> sortAndMergePlans(List<LogicalPlan> plans, DrillConfig config) throws Exception {
        Map<LogicalPlan, LogicalPlan> resultMap = new HashMap<>();
        /**
         * sort into sortedByProjectID, by projectID
         */
        sortPlanByProjectId(plans);
        Map<String, ProjectMergeContext> projectCtxMap = splitByTableName();
        markMergability(projectCtxMap, MergeType.same);
        resultMap = mergePlans(projectCtxMap);
        return resultMap;
    }

    Comparator<ScanWithPlan> swpComparator = new Comparator<ScanWithPlan>() {
        @Override
        public int compare(ScanWithPlan o1, ScanWithPlan o2) {
            return Bytes.compareTo(o1.range.getStartRowKey(), o2.range.getStartRowKey());
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
    private Map<LogicalPlan, LogicalPlan> splitScanByRowKey(List<LogicalPlan> origPlans, DrillConfig config) throws Exception {
      //原始plan按project id分堆
      sortPlanByProjectId(origPlans);

      logger.info("Enter split Scan by Rk");
      Map<String, ProjectMergeContext> projectCtxMap = splitByTableName();
      List<LogicalPlan> resultPlans = new ArrayList<>();
      Map<LogicalPlan, LogicalPlan> resultPlanMap = new HashMap<>();

        for (Map.Entry<String, ProjectMergeContext> entry : projectCtxMap.entrySet()) {
            String projectId = entry.getKey();
            ProjectMergeContext ctx = entry.getValue();
            Map<String, Set<ScanWithPlan>> tableName2Scans = ctx.tableName2Plans;
            Map<ScanWithPlan, List<LogicalOperator>> scan2Los = new HashMap<>();
            long srt=System.currentTimeMillis(),t1=System.currentTimeMillis(),t2;
            for (Map.Entry<String, Set<ScanWithPlan>> entry1 : tableName2Scans.entrySet()) {
                String tableName = entry1.getKey();
                if (!tableName.contains(QueryMasterConstant.DEU)) continue;
                ScanWithPlan[] swps = new ArrayList<>(entry1.getValue()).toArray
                        (new ScanWithPlan[entry1.getValue().size()]);
                Arrays.sort(swps, swpComparator);

                byte[][] rkPoints = new byte[swps.length*2][];
                Map<RowKeyRange, List<ScanWithPlan>> crosses = new HashMap<>();
                Map<ScanWithPlan, List<RowKeyRange>> scanSplits = new HashMap<>();
                for (int i = 0; i < swps.length; i++) {
                    rkPoints[i * 2] = swps[i].range.getStartRowKey();
                    rkPoints[i * 2 + 1] = swps[i].range.getEndRowKey();
                }

              //对row key point排序并去重，构造出key range段
              Arrays.sort(rkPoints, Bytes.BYTES_COMPARATOR);
              List<RowKeyRange> origRangeList = new ArrayList<>();
              // pick row key ranges for every scan and reverse
              for(int i=0; i<rkPoints.length-1; i++){
                if(Bytes.compareTo(rkPoints[i+1], rkPoints[i])>0) {
                  origRangeList.add(new RowKeyRange(rkPoints[i],rkPoints[i+1]));
                }
              }
              RowKeyRange [] ranges=origRangeList.toArray(new RowKeyRange[origRangeList.size()]);

              t1 = System.currentTimeMillis();
              int index = 0;
              for(int j=0; j<swps.length; j++){
                  boolean intoScan = false;
                  for(int i=index; i<ranges.length; i++){
                      if(LogicalPlanUtil.isRkRangeInScan(ranges[i].getEndRowKey(), swps[j])){
                        intoScan = true;
                        List<ScanWithPlan> swpList = crosses.get(ranges[i]);
                        if(null == swpList){
                            swpList = new ArrayList<>();
                            crosses.put(ranges[i], swpList);
                        }
                        swpList.add(swps[j]);
                        List<RowKeyRange> rangeList = scanSplits.get(swps[j]);
                        if (rangeList == null) {
                            rangeList = new ArrayList<>();
                            scanSplits.put(swps[j], rangeList);
                        }
                          rangeList.add(ranges[i]);
                      }else if(!intoScan){
                          index++;
                      }
                  }
              }
              t2 = System.currentTimeMillis();
              logger.info("rkPoint sort using "+(t2-t1)+" ms");

              t1 = System.currentTimeMillis();
              //crosses: range => scans; scanSplits: scan => ranges
              //slice scan to union(smaller scans)
              for (Map.Entry<ScanWithPlan, List<RowKeyRange>> entry2 : scanSplits.entrySet()) {
                ScanWithPlan swp = entry2.getKey();
                List<RowKeyRange> rangeList = entry2.getValue();
                if (rangeList.size() == 1 && crosses.get(rangeList.get(0)).size() == 1) {
                  //一个scan只对应一个key range
                  continue;
                }
                List<LogicalOperator> unionInputs = new ArrayList<>();
                List<LogicalOperator> operators = new ArrayList<>();

                // get union--->List(project-->filter--->scan)
                for (RowKeyRange range : rangeList) {
                  List<ScanWithPlan> swpList = crosses.get(range);
                  Scan baseScan = LogicalPlanUtil.getBaseScan(range, swpList, config);
                  JsonNode rkNode = baseScan.getSelection().getRoot().get(0).get(SELECTION_KEY_WORD_ROWKEY);
                  String srk = rkNode.get(SELECTION_KEY_WORD_ROWKEY_START).textValue();
                  String enk = rkNode.get(SELECTION_KEY_WORD_ROWKEY_END).textValue();
                  if(srk.equals(enk)) {
                    logger.error("Start key equals to end key. " + srk);
                  }
                  if (swpList.size() == 1) {
                    unionInputs.add(baseScan);
                    operators.add(baseScan);
                  } else {
                      operators.add(baseScan);
                      Filter filter = LogicalPlanUtil.getFilter(baseScan,swp.scan, config);
                    if (filter != null) {
                      filter.setInput(baseScan);
                      if (LogicalPlanUtil.getProjectionEntry(swp.scan, config).size() <
                              LogicalPlanUtil.getProjectionEntry(baseScan, config).size()) {
                          Project project = LogicalPlanUtil.getProject(swp.scan, config);
                          project.setInput(filter);
                          operators.add(filter);
                          operators.add(project);
                          unionInputs.add(project);
                        } else {
                            operators.add(filter);
                            unionInputs.add(filter);
                        }
                    } else {
                        if (LogicalPlanUtil.getProjectionEntry(swp.scan, config).size() <
                                LogicalPlanUtil.getProjectionEntry(baseScan, config).size()) {
                            Project project = LogicalPlanUtil.getProject(swp.scan, config);
                            project.setInput(baseScan);
                            operators.add(project);
                            unionInputs.add(project);
                        } else {
                            unionInputs.add(baseScan);
                        }
                    }
                  }
                }
                if (unionInputs.size() != 1) {
                    Union union = new Union(unionInputs.toArray(new LogicalOperator[unionInputs.size()]), false);
                    operators.add(union);
                }
                scan2Los.put(swp, operators);
              }
              t2 = System.currentTimeMillis();
              logger.info("produce subs lp using " + (t2-t1) + " ms");
            }
            long endTime = System.currentTimeMillis();
            logger.info("sort rkPoints and produce subsitute lp  using " + (endTime-srt) + " ms");
            //substitude the big scan with the union
            for (LogicalPlan plan : sortedByProjectID.get(projectId)) {
              List<LogicalOperator> operators = plan.getSortedOperators();
              Set<ScanWithPlan> swps = ctx.plan2Swps.get(plan);
              for (ScanWithPlan swp : swps) {
                String tableName = LogicalPlanUtil.getTableName(swp.scan);
                if (!tableName.contains(QueryMasterConstant.DEU)) continue;
                List<LogicalOperator> targetOperators = scan2Los.get(swp);
                if (targetOperators != null) {
                    for (LogicalOperator lo : LogicalPlanUtil.getParents(swp.scan, plan)) {
                        LogicalPlanUtil.substituteInParent(swp.scan,
                                targetOperators.get(targetOperators.size() - 1), lo);

                    }
                    operators.addAll(targetOperators);
                    operators.remove(swp.scan);
                }
              }
              PlanProperties head = plan.getProperties();
              Map<String, StorageEngineConfig> storageEngines = plan.getStorageEngines();
              LogicalPlan resultPlan = new LogicalPlan(head, storageEngines, operators);
              resultPlans.add(resultPlan);
              resultPlanMap.put(plan, resultPlan);
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
  private Map<LogicalPlan, LogicalPlan> mergeToBigScan(List<LogicalPlan> plans, DrillConfig config) throws Exception {
        Map<LogicalPlan, LogicalPlan> result = new HashMap<LogicalPlan, LogicalPlan>();
        sortPlanByProjectId(plans);
        Map<String, ProjectMergeContext> projectCtxMap = splitByTableName();
        markMergability(projectCtxMap, MergeType.sametable);
        result = producePlansWithUniondScan(projectCtxMap, config);
        return result;
    }


  /**
   * merge all the scans that belong to the same project to a UnionedScan, invoked by mergeToBigScan
   * @param projectCtxMap
   * @param config
   * @return plans with UnionedScan
   * @throws IOException
   */  
    private Map<LogicalPlan, LogicalPlan> producePlansWithUniondScan(Map<String, ProjectMergeContext> projectCtxMap, DrillConfig config) throws IOException {
        Map<LogicalPlan, LogicalPlan> result = new HashMap<>();
        for (Map.Entry<String, List<LogicalPlan>> entry : sortedByProjectID.entrySet()) {
            String project = entry.getKey();
            ProjectMergeContext ctx = projectCtxMap.get(project);
            //开始合并
            ctx.planInspector = new ConnectivityInspector<LogicalPlan, DefaultEdge>(ctx.mergePlanSets);
            List<Set<LogicalPlan>> mergeSets = ctx.planInspector.connectedSets();
            ctx.scanInspector = new ConnectivityInspector<Scan, DefaultEdge>(ctx.mergedScanSets);
            ctx.devidedScanSets = ctx.scanInspector.connectedSets();

            for (int i = 0; i < mergeSets.size(); i++) {
                Set<LogicalPlan> planSet = mergeSets.get(i);
                LogicalPlan mergedPlan = produceBigScan(planSet, ctx, config);
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
   * produce UnionedScan for one project, invoked by producePlansWithUniondScan
   * @param plans
   * @param projectCtx
   * @param config
   * @return
   * @throws IOException
   */
  private LogicalPlan produceBigScan(Set<LogicalPlan> plans, ProjectMergeContext projectCtx, DrillConfig config) throws IOException {

        List<LogicalOperator> roots = new ArrayList<>();
        for (LogicalPlan plan : plans) {
            Collection<SinkOperator> unitRoots = plan.getGraph().getRoots();
            roots.addAll(unitRoots);
        }
        //PlanMergeContext planCtx = new PlanMergeContext();
        List<LogicalOperator> operators = new ArrayList<>();
        Map<Scan, UnionedScanSplit> substituteMap = new HashMap<>();
        ScanRkCompartor scanRkCompartor = new ScanRkCompartor();
        for (Map.Entry<String, Set<ScanWithPlan>> entry : projectCtx.tableName2Plans.entrySet()) {
            //String tableName=entry.getKey();

            Set<ScanWithPlan> swps = entry.getValue();
            if (swps.size() <= 1)
                continue;
            ScanWithPlan[] swpArr = new ArrayList<ScanWithPlan>(swps).toArray(new ScanWithPlan[swps.size()]);
            if (!plans.contains(swpArr[0].plan))
                continue;
            if (!swpArr[0].scan.getStorageEngine().contains("hbase"))
                continue;
            ObjectMapper mapper = config.getMapper();
            ArrayNode jsonArray = new ArrayNode(mapper.getNodeFactory());
            //List<Map<String,Object>> mapList=new ArrayList<Map<String,Object>>();
            Arrays.sort(swpArr, scanRkCompartor);
            for (int i = 0; i < swpArr.length; i++) {
                for (JsonNode node : swpArr[i].scan.getSelection().getRoot()) {
                    jsonArray.add(node);
                }
            }
            String selectionStr = mapper.writeValueAsString(jsonArray);
            JSONOptions selection = mapper.readValue(selectionStr, JSONOptions.class);
            UnionedScan unionedScan = new UnionedScan(swpArr[0].scan.getStorageEngine(), selection, swpArr[0].scan.getOutputReference());
            List<String> memos = new ArrayList<>();

            for (int i = 0; i < swpArr.length; i++) {
                int[] entries = new int[1];
                entries[0] = i;
                ScanWithPlan swp = swpArr[i];
                if (swp.scan.getMemo() != null) {
                    memos.add(swp.scan.getMemo());
                } else {
                    memos.add("n/a");
                }
                UnionedScanSplit unionedScanSplit = new UnionedScanSplit(entries);
                unionedScanSplit.setMemo(Arrays.toString(entries));

                unionedScanSplit.setInput(unionedScan);
                LogicalPlan plan = swpArr[i].plan;
                AdjacencyList<LogicalOperator> adjacencyList = plan.getGraph().getAdjList().getReversedList();
                for (AdjacencyList<LogicalOperator>.Node lo : adjacencyList.getInternalRootNodes()) {
                    if (lo.getNodeValue().equals(swpArr[i].scan)) {
                        for (Edge<AdjacencyList<LogicalOperator>.Node> edge : adjacencyList.getAdjacent(lo)) {
                            AdjacencyList<LogicalOperator>.Node parent = edge.getTo();
                            LogicalOperator parentLo = parent.getNodeValue();
                            LogicalPlanUtil.substituteInParent(swpArr[i].scan, unionedScanSplit, parentLo);

                        }
                    }
                }

                substituteMap.put(swpArr[i].scan, unionedScanSplit);
            }
            unionedScan.setMemo(memos.toString());

            operators.add(unionedScan);

        }
        PlanProperties head = null;
        Map<String, StorageEngineConfig> se = null;
        for (LogicalPlan plan : plans) {
            if (head == null)
                head = plan.getProperties();
            if (se == null)
                se = plan.getStorageEngines();
            List<LogicalOperator> origPlanOperators = plan.getSortedOperators();
            for (LogicalOperator lo : origPlanOperators) {
                if (substituteMap.containsKey(lo)) {
                    LogicalOperator target = substituteMap.get(lo);
                    if (!operators.contains(target))
                        operators.add(substituteMap.get(lo));
                } else {
                    if (!operators.contains(lo))
                        operators.add(lo);
                }
            }
        }

        if (roots.size() > 1) {
            List<LogicalOperator> unionInputs = new ArrayList<>();
            //Store store=null;
            for (LogicalOperator root : roots) {
                if (root instanceof Store) {
                    unionInputs.add(((Store) root).getInput());
                    ((Store) root).setInput(null);
                    operators.remove(root);
                } else {
                    unionInputs.add(root);
                }
            }
            Union union = new Union(unionInputs.toArray(new LogicalOperator[unionInputs.size()]), false);
            Store store = new Store(((Store) roots.get(0)).getStorageEngine(), ((Store) roots.get(0)).getTarget(), ((Store) roots.get(0)).getPartition());
            store.setInput(union);
            operators.add(union);
            operators.add(store);
        }

        return new LogicalPlan(head, se, operators);
    }

  /**
   * Slice the scan that has multi projections into one or more smaller scans, one smaller scan corresponding to one projection.
   * The projections of a scan were generated by qm according to the original star/end row key and events that related.   
   * @param plans
   * @param config
   * @return original logicplan => tranlated logicalplan
   * @throws IOException
   */
    public Map<LogicalPlan, LogicalPlan> splitBigScan(List<LogicalPlan> plans, DrillConfig config) throws IOException {
        splitedPlans = new ArrayList<>();
        splitedPlan2Orig = new HashMap<>();
        Map<LogicalPlan, LogicalPlan> results = new HashMap<>();
        int index = 0;
        for (LogicalPlan plan : plans) {
            List<LogicalOperator> operators = new ArrayList<>();
            Map<LogicalOperator, LogicalOperator> scanReplaceMap = new HashMap<>();
            for (LogicalOperator leaf : plan.getGraph().getLeaves()) {
                if (leaf instanceof Scan) {
                    Scan origScan = (Scan) leaf;
                    List<Scan> childScans = new ArrayList<>();
                    if (origScan.getStorageEngine().contains(QueryMasterConstant.HBASE)) {
                        int selectionSize = origScan.getSelection().getRoot().size();
                        if(selectionSize < 2) continue;
                        for(JsonNode selectionNode : origScan.getSelection().getRoot()){
                            ArrayNode rootNode = new ArrayNode(JsonNodeFactory.instance);
                            rootNode.add(selectionNode);
                            ObjectMapper mapper = config.getMapper();
                            String optionStr = mapper.writeValueAsString(rootNode);
                            JSONOptions option = mapper.readValue(optionStr, JSONOptions.class);
                            Scan childScan = new Scan(origScan.getStorageEngine(), option, origScan.getOutputReference());
                            childScans.add(childScan);
                        }
                    }
                    if(childScans.size() < 2) continue;
                    operators.addAll(childScans);
                    // construct the union
                    Union union = new Union(childScans.toArray(new LogicalOperator[childScans.size()]), false);
                    scanReplaceMap.put(origScan, union);
                    for (LogicalOperator parent : LogicalPlanUtil.getParents(origScan, plan)) {
                        LogicalPlanUtil.substituteInParent(origScan, union, parent);
                    }
                }
            }

            for (LogicalOperator op : plan.getSortedOperators()) {
                LogicalOperator subs=scanReplaceMap.get(op);
                if(subs!=null)
                    operators.add(subs);
                else
                    operators.add(op);
            }

            PlanProperties head = plan.getProperties();
            Map<String, StorageEngineConfig> se = plan.getStorageEngines();
            index++;
            try {
                LogicalPlan subsPlan = new LogicalPlan(head, se, operators);
                splitedPlans.add(subsPlan);
                splitedPlan2Orig.put(subsPlan, plan);
                results.put(plan, subsPlan);
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
  private void sortPlanByProjectId(List<LogicalPlan> plans) {
        sortedByProjectID = new HashMap<String, List<LogicalPlan>>();
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
  private Map<String, ProjectMergeContext> splitByTableName() throws Exception {
        Map<String, ProjectMergeContext> projectCtxMap = new HashMap<String, ProjectMergeContext>();
        for (Map.Entry<String, List<LogicalPlan>> entry : sortedByProjectID.entrySet()) {
          String project = entry.getKey();
          List<LogicalPlan> plans = entry.getValue();

          ProjectMergeContext ctx = new ProjectMergeContext();
          projectCtxMap.put(project, ctx);
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
                      Set<ScanWithPlan> scanWithPlans = ctx.plan2Swps.get(plan);
                      if (scanWithPlans == null) {
                          scanWithPlans = new HashSet<>();
                          ctx.plan2Swps.put(plan, scanWithPlans);
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

    private Map<String, ProjectMergeContext> markMergability(
            Map<String, ProjectMergeContext> projectCtxMap, LogicalPlanUtil.MergeType option) {
        Map<String, ProjectMergeContext> result = new HashMap<>();
        for (Map.Entry<String, List<LogicalPlan>> entry : sortedByProjectID.entrySet()) {
            String project = entry.getKey();
            ProjectMergeContext ctx = projectCtxMap.get(project);
            result.put(project, ctx);
            //检查scan，标记合并
            for (Map.Entry<String, Set<ScanWithPlan>> entry2 : ctx.tableName2Plans.entrySet()) {
                String tableName = entry2.getKey();
                ScanWithPlan[] swps = new ScanWithPlan[entry2.getValue().size()];
                entry2.getValue().toArray(swps);
                for (int i = 0; i < swps.length; i++) {
                    ScanWithPlan swpFrom = swps[i];
                    for (int j = i + 1; j < swps.length; j++) {
                        ScanWithPlan swpTo = swps[j];
                        LogicalPlanUtil.Mergeability<Scan> mergeability = null;
                        switch (option) {
                            case same:
                                mergeability = LogicalPlanUtil.mergeable(swpFrom.scan, swpTo.scan);
                                break;
                            case sametable:
                                mergeability = new LogicalPlanUtil.Mergeability<Scan>(MergeType.sametable, swpFrom.scan, swpTo.scan);
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
  private Map<LogicalPlan, LogicalPlan> mergePlans(Map<String, ProjectMergeContext> projectCtxMap) {
        Map<LogicalPlan, LogicalPlan> mergeResult = new HashMap<>();
        for (Map.Entry<String, List<LogicalPlan>> entry : sortedByProjectID.entrySet()) {
            String project = entry.getKey();
            ProjectMergeContext ctx = projectCtxMap.get(project);
            //开始合并
            ctx.planInspector = new ConnectivityInspector<LogicalPlan, DefaultEdge>(ctx.mergePlanSets);
            List<Set<LogicalPlan>> mergeSets = ctx.planInspector.connectedSets();
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
     * 合并两个plan的方法：
     * <p/>
     * 如果两个scan完全一样，就合并。 如果两个scan，其中一个的scan被另一个包含，就给他加一个filter，然后合并。 非叶子节点：如果完全一样（子节点也一样），就合并。
     */
    private LogicalPlan doMergePlan(Set<LogicalPlan> plans, ProjectMergeContext projectCtx) {
   /*
   if (plans.size() == 1) {
      //no need to run merge; should not run merge
      return plans.iterator().next();
    }
    */
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
        if (swpFrom.plan != swpTo.plan)
            ctx.mergePlanSets.addEdge(swpFrom.plan, swpTo.plan);
        ctx.mergedScanSets.addVertex(mergeability.from);
        ctx.mergedScanSets.addVertex(mergeability.to);
        if (mergeability.from != mergeability.to)
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
        Map<LogicalPlan, Set<ScanWithPlan>> plan2Swps = new HashMap<>();
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
        public RowKeyRange range;

        ScanWithPlan() {
        }

        ScanWithPlan(Scan scan, LogicalPlan plan, String tableName) {
          this.scan = scan;
          this.plan = plan;
          this.tableName = tableName;
          if (scan.getStorageEngine().equals(QueryMasterConstant.HBASE)) {
            this.range = LogicalPlanUtil.getRowKeyRange(scan);
          }
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
    public static Map<LogicalPlan, LogicalPlan> sortAndMerge(List<LogicalPlan> plans, DrillConfig config) throws Exception {
      long begin = System.nanoTime();
      boolean writePlan = QMConfig.conf().getBoolean(QMConfig.WRITE_PLAN, false);
      PlanWriter pw = null;
      if (writePlan) {
        pw = new PlanWriter(System.currentTimeMillis(), config);
        pw.writeSourcePlans(plans);
      }

      PlanMerge planMerge = new PlanMerge(plans);

      //对每个hbase scan生成具体的start key和end key
      long st = System.currentTimeMillis();
      transferPlan(plans,config);
      logger.info("Transfer using " + (System.currentTimeMillis() - st) + " ms");

      //对每天拆成一个scan，scan和原始parent之前增加一个union
      st = System.currentTimeMillis();
      Map<LogicalPlan, LogicalPlan> splitBigPlanMap = planMerge.splitBigScan(plans, config);
      logger.info("split big scan using " + (System.currentTimeMillis() - st) + " ms");

      //按每个scan的key range拆分合并
      st = System.currentTimeMillis();
      List<LogicalPlan> bigPlanSplitedPlans = new ArrayList<>(splitBigPlanMap.values());
      Map<LogicalPlan, LogicalPlan> splitRkPlanMap = planMerge.splitScanByRowKey(bigPlanSplitedPlans, config);
      logger.info("split Scan by Rk using " + (System.currentTimeMillis() - st) + " ms");

      List<LogicalPlan> rkSplitedPlans = new ArrayList<>(splitRkPlanMap.values());

      //对相同的操作符进行合并
      st = System.currentTimeMillis();
      Map<LogicalPlan, LogicalPlan> mergePlanMap = planMerge.sortAndMergePlans(rkSplitedPlans, config);
      logger.info("merge plan using " + (System.currentTimeMillis() - st) + " ms");

      st = System.currentTimeMillis();
      Set<LogicalPlan> scanMergedPlanSet = new HashSet<>(mergePlanMap.values());
      List<LogicalPlan> scanMergedPlans = new ArrayList<>(scanMergedPlanSet);
      Map<LogicalPlan, LogicalPlan> mergeToTableScanMap = planMerge.mergeToBigScan(scanMergedPlans, config);
      logger.info("merge to big scan using " + (System.currentTimeMillis() - st) + " ms");

      Map<LogicalPlan, LogicalPlan> result = new HashMap<>();
      for (Map.Entry<LogicalPlan, LogicalPlan> entry : splitBigPlanMap.entrySet()) {
          LogicalPlan orig = entry.getKey();
          LogicalPlan splitBigScanResultPlan = splitBigPlanMap.get(entry.getKey());
          LogicalPlan splitRkResultPlan = splitRkPlanMap.get(splitBigScanResultPlan);
          LogicalPlan mergePlanResultPlan = mergePlanMap.get(splitRkResultPlan);
          LogicalPlan mergeToTableScanResultPlan = mergeToTableScanMap.get(mergePlanResultPlan);
          result.put(orig, mergeToTableScanResultPlan);
      }

      if (writePlan) {
        pw.writeMergedPlan(mergeToTableScanMap.values());
      }

      logger.info("------Plan merge taken: " + (System.nanoTime()-begin)/1.0e9 + " sec");
      return result;
    }

    public Map<LogicalPlan, LogicalPlan> getMerged() {
        return merged;
    }



}
