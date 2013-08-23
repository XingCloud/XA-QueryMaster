package com.xingcloud.qm.service;

import static org.apache.drill.common.util.DrillConstants.SE_HBASE;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_FILTERS;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_PROJECTIONS;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_ROWKEY;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_ROWKEY_END;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_ROWKEY_START;
import static org.apache.drill.common.util.Selections.SELECTION_KEY_WORD_TABLE;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.PlanProperties;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.graph.AdjacencyList;
import org.apache.drill.common.graph.Edge;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.logical.StorageEngineConfig;
import org.apache.drill.common.logical.data.Join;
import org.apache.drill.common.logical.data.LogicalOperator;
import org.apache.drill.common.logical.data.Scan;
import org.apache.drill.common.logical.data.SingleInputOperator;
import org.apache.drill.common.logical.data.SourceOperator;
import org.apache.drill.common.logical.data.Store;
import org.apache.drill.common.logical.data.Union;
import org.jgrapht.DirectedGraph;
import org.jgrapht.UndirectedGraph;
import org.jgrapht.alg.ConnectivityInspector;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.graph.SimpleGraph;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PlanMerge {

  private final List<LogicalPlan> incoming;

  private List<LogicalPlan> splitedPlans;

  private Map<LogicalPlan, LogicalPlan> splitedPlan2Orig;

  private Map<LogicalPlan, LogicalPlan> merged;

  private Map<String, List<LogicalPlan>> sortedByProjectID;

  public PlanMerge(List<LogicalPlan> plans) throws Exception {
    this.incoming = plans;
    //splitBigScan();
    //sortAndMerge();
  }

  public List<LogicalPlan> getSplitedPlans() {
    return splitedPlans;
  }

  public void splitBigScan() throws IOException {
    splitedPlans = new ArrayList<>();
    splitedPlan2Orig = new HashMap<LogicalPlan, LogicalPlan>();
    int index = 0;
    for (LogicalPlan plan : incoming) {
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
              ObjectMapper mapper = DrillConfig.create().getMapper();
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

          Scan[] splitedScans = new Scan[childScans.size()];
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
      } catch (Exception e) {
        System.out.println(index + " plan has problem");
        e.printStackTrace();
      }
    }
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

  private void sortAndMerge() throws Exception {
    merged = new HashMap<LogicalPlan, LogicalPlan>();
    sortedByProjectID = new HashMap<String, List<LogicalPlan>>();
    /**
     * sort into sortedByProjectID, by projectID
     */
    //for (LogicalPlan plan : incoming) {
    for (LogicalPlan plan : splitedPlans) {
      String projectID = getProjectID(plan);
      List<LogicalPlan> projectPlans = sortedByProjectID.get(projectID);
      if (projectPlans == null) {
        projectPlans = new ArrayList<>();
        sortedByProjectID.put(projectID, projectPlans);
      }
      projectPlans.add(plan);
    }
    for (Map.Entry<String, List<LogicalPlan>> entry : sortedByProjectID.entrySet()) {
      String project = entry.getKey();
      List<LogicalPlan> plans = entry.getValue();

      ProjectMergeContext ctx = new ProjectMergeContext();
      for (LogicalPlan plan : plans) {
        //init merging plan sets:初始状态每个plan自成一组
        ctx.mergePlanSets.addVertex(plan);
//        ctx.mergeID2plans.put(mergeID, new HashSet<LogicalPlan>(Arrays.asList(plan)));
//        ctx.plan2MergeID.put(plan, mergeID);
        //找到有对相同table操作的scan
        Collection<SourceOperator> leaves = plan.getGraph().getLeaves();
        for (SourceOperator leaf : leaves) {
          if (leaf instanceof Scan) {
            Scan scan = (Scan) leaf;
            //初始状态，每个scan自成一组
            ctx.mergedScanSets.addVertex(scan);
            String tableName = getTableName(scan);
            ScanWithPlan swp = new ScanWithPlan(scan, plan, tableName);
            Set<ScanWithPlan> swps = ctx.tableName2Plans.get(tableName);
            if (swps == null) {
              swps = new HashSet<>();
              ctx.tableName2Plans.put(tableName, swps);
            }
            swps.add(swp);
            Set<String> tableNames = ctx.plan2TableNames.get(plan);
            if (tableNames == null) {
              tableNames = new HashSet<>();
              ctx.plan2TableNames.put(plan, tableNames);
            }
            tableNames.add(tableName);
          }
        }
      }

      //检查scan，标记合并
      for (Map.Entry<String, Set<ScanWithPlan>> entry2 : ctx.tableName2Plans.entrySet()) {
        String tableName = entry2.getKey();
        ScanWithPlan[] swps = new ScanWithPlan[entry2.getValue().size()];
        entry2.getValue().toArray(swps);
        for (int i = 0; i < swps.length; i++) {
          ScanWithPlan swpFrom = swps[i];
          for (int j = i + 1; j < swps.length; j++) {
            ScanWithPlan swpTo = swps[j];
            Mergeability<Scan> mergeability = mergeable(swpFrom.scan, swpTo.scan);
            if (mergeability != null) {
              markMerge(swpFrom, swpTo, mergeability, ctx);
            }
          }
        }
      }
      //开始合并
      ctx.planInspector = new ConnectivityInspector<LogicalPlan, DefaultEdge>(ctx.mergePlanSets);
      List<Set<LogicalPlan>> mergeSets = ctx.planInspector.connectedSets();
      ctx.scanInspector = new ConnectivityInspector<Scan, DefaultEdge>(ctx.mergedScanSets);
      ctx.devidedScanSets = ctx.scanInspector.connectedSets();

      for (int i = 0; i < mergeSets.size(); i++) {
        Set<LogicalPlan> planSet = mergeSets.get(i);
        LogicalPlan mergedPlan = doMergePlan(planSet, ctx);
        for (LogicalPlan original : planSet) {
          LogicalPlan realOrig = splitedPlan2Orig.get(original);
          merged.put(realOrig, mergedPlan);
        }
      }
      ctx.close();
    }//for sortedByProjectID
  }

  /**
   * 合并两个plan的方法：
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
              if (equals(op, otherParent) != null) {
                //found someone to merge
                mergeTo = otherParent;
                break childrenLoop;
              }
            }
          }
          if (mergeTo == null && planCtx.mergeResult.size() >= 6) {
            System.out.println("store merge to plan");
          }
          doMergeOperator(op, mergeTo, planCtx);

          lookForParentsAndSubstitute(opNode, child2Parents, nextStepSet, mergeTo);
        }
      }
      System.out.println("merge plan one");
      System.out.println("--------------------------------------\n" +
                           "-------------------------------------------\n" +
                           "-------------------------------------------------------");
      try {
        System.out.println(
          "merge result: \n" + new LogicalPlan(head, se, planCtx.mergeResult).toJsonString(DrillConfig.create()));
      } catch (Exception e) {
        e.printStackTrace();
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
          System.err.println("noooo!");
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
        substituteInParent(child.getNodeValue(), substitutionInParents, parent);
      }
    }
  }

  private void substituteInParent(LogicalOperator source, LogicalOperator target, LogicalOperator parent) {
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

  private void markMerge(ScanWithPlan swpFrom, ScanWithPlan swpTo, Mergeability<Scan> mergeability,
                         ProjectMergeContext ctx) {
    ctx.mergePlanSets.addVertex(swpFrom.plan);
    ctx.mergePlanSets.addVertex(swpTo.plan);
    ctx.mergePlanSets.addEdge(swpFrom.plan, swpTo.plan);
    ctx.mergedScanSets.addVertex(mergeability.from);
    ctx.mergedScanSets.addVertex(mergeability.to);
    ctx.mergedScanSets.addEdge(mergeability.from, mergeability.to);

  }

  private Mergeability<LogicalOperator> equals(LogicalOperator op1, LogicalOperator op2) {
    if (LOPComparator.equals(op1, op2)) {
      return new Mergeability<>(MergeType.same, op1, op2);
    }
    return null;
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

  /**
   * 判断两个plan是否合并： 如果两个plan有相同的scan，那就合并。 如果两个plan，其中一个的scan范围包含另外一个，那就合并。 TODO 现在只合并一模一样的scan。需要更多的merge的策略和相应的数据
   *
   * @param scan1
   * @param scan2
   * @return
   */
  private Mergeability<Scan> mergeable(Scan scan1, Scan scan2) {
    return LOPComparator.equals(scan1, scan2) ? new Mergeability<Scan>(MergeType.same, scan1, scan2) : null;
  }

  private Mergeability<Scan> equals(Scan scan1, Scan scan2) throws Exception {
    if (getTableName(scan1).equals(getTableName(scan2)) && scan1.getSelection().equals(scan2.getSelection())) {
      return new Mergeability<>(MergeType.same, scan1, scan2);
    }
    return null;
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

  static class ScanWithPlan {
    Scan scan;
    LogicalPlan plan;
    String tableName;

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

  public static String getTableName(Scan scan) throws Exception {
    try {
      String storageEngine = scan.getStorageEngine();
      if (SE_HBASE.equals(storageEngine)) {
        return scan.getSelection().getRoot().get(0).get("table").asText();
      } else {
        return scan.getSelection().getRoot().get("table").asText();
      }
    } catch (Exception e) {
      throw new Exception(e);
    }
  }

  /**
   * @param plans 需要merge的LogicalPlan 列表
   * @return 原始的LogicalPlan和合并以后LogicalPlan之间的对应关系。 Map的key 是 原始的LogicalPlan，value是合并后的LogicalPlan。 如果输入的plans当中，有plan
   *         没有和别的plan合并，则在返回的map中，key和value都是这个plan。
   */
  public static Map<LogicalPlan, LogicalPlan> sortAndMerge(List<LogicalPlan> plans) throws Exception {
    PlanMerge planMerge = new PlanMerge(plans);
    planMerge.splitBigScan();
      /*
      for(LogicalPlan plan : planMerge.getSplitedPlans()){

          String planStr=DrillConfig.create().getMapper().writeValueAsString(plan);
          System.out.println(planStr);
      }
      */
    planMerge.sortAndMerge();
    return planMerge.getMerged();
  }

  public Map<LogicalPlan, LogicalPlan> getMerged() {
    return merged;
  }

  static class Mergeability<T extends LogicalOperator> {

    MergeType mergeType;
    T from;
    T to;

    Mergeability() {
    }

    Mergeability(MergeType mergeType, T from, T to) {
      this.mergeType = mergeType;
      this.from = from;
      this.to = to;
    }
  }

  static enum MergeType {
    same, belongsto
  }
}
