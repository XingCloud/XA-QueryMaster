package com.xingcloud.qm.service;

import org.apache.drill.common.logical.data.*;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

public abstract class LOPComparator<T extends LogicalOperator> implements Comparator<T>{
  private static final Map<Class<? extends LogicalOperator>, LOPComparator> comparators = new HashMap<Class<? extends LogicalOperator>, LOPComparator>();
  static{
    comparators.put(SingleInputOperator.class, new SingleInputComparator());
    comparators.put(Distinct.class, new DistinctComparator());
    comparators.put(Filter.class, new FilterComparator());
    comparators.put(Join.class, new JoinComparator());
    comparators.put(Limit.class, new LimitComparator());
    comparators.put(Project.class, new ProjectComparator());
    comparators.put(CollapsingAggregate.class, new CollapsingAggregateComparator());
    comparators.put(Scan.class, new ScanComparator());
    comparators.put(Segment.class, new SegmentComparator());
    comparators.put(Union.class, new UnionComparator());
  }
  public static boolean equals(LogicalOperator op1, LogicalOperator op2) {
    if(op1 == op2){
      return true;
    }
    if(op1 == null || op2==null){
      return false;
    }
    Class<? extends LogicalOperator> clz = op1.getClass();
    if(!clz.equals(op2.getClass())){
      return false;
    }
    LOPComparator c = comparators.get(clz);
    if(c == null){
      return false;
    }
    return c.compare(op1, op2) == 0;
    
  }

  @Override
  public boolean equals(Object obj) {
    return super.equals(obj);  
  }

  private static class UnionComparator extends LOPComparator<Union> {
    @Override
    public int compare(Union o1, Union o2) {
      LogicalOperator[] in1 = o1.getInputs();
      LogicalOperator[] in2 = o2.getInputs();
      if(in1.length != in2.length){
        return -1;
      }
      for (int i = 0; i < in1.length; i++) {
        LogicalOperator child1 = in1[i];
        LogicalOperator child2 = in2[i];
        if(!equals(child1, child2)){
          return -1;
        }
      }
      return 0;
    }
  }

  private static class DistinctComparator extends LOPComparator<Distinct> {
    @Override
    public int compare(Distinct o1, Distinct o2) {
      if(0!=comparators.get(SingleInputOperator.class).compare(o1, o2)){
        return -1;
      }
      if (o1.getRef() != null ? !o1.getRef().equals(o2.getRef()) : o2.getRef() != null) return -1;
      if (o1.getWithin() != null ? !o1.getWithin().equals(o2.getWithin()) : o2.getWithin() != null) return -1;
      return 0;
    }
  }

  private static class FilterComparator extends LOPComparator<Filter> {
    @Override
    public int compare(Filter o1, Filter o2) {
      if(0!=comparators.get(SingleInputOperator.class).compare(o1, o2)){
        return -1;
      }
      if (o1.getExpr() != null ? !o1.getExpr().equals(o2.getExpr()) : o2.getExpr() != null) return -1;
      return 0;
    }
  }

  private static class JoinComparator extends LOPComparator<Join> {
    @Override
    public int compare(Join o1, Join o2) {
      if (o1.getJointType() != o2.getJointType()) return -1;
      if (!Arrays.equals(o1.getConditions(), o2.getConditions())) return -1;
      if (!equals(o1.getLeft(), o2.getLeft())) return -1;
      if (!equals(o1.getRight(), o2.getRight())) return -1;
      return 0; 
    }
  }

  private static class LimitComparator extends LOPComparator<Limit> {
    @Override
    public int compare(Limit o1, Limit o2) {
      return -1;
    }
  }

  private static class ProjectComparator extends LOPComparator<Project> {
    @Override
    public int compare(Project o1, Project o2) {
      if(0!=comparators.get(SingleInputOperator.class).compare(o1, o2)){
        return -1;
      }
      if (!Arrays.equals(o1.getSelections(), o2.getSelections())) return -1;
      return 0;
    }
  }

  private static class CollapsingAggregateComparator extends LOPComparator<CollapsingAggregate> {
    @Override
    public int compare(CollapsingAggregate o1, CollapsingAggregate o2) {
      if(0!=comparators.get(SingleInputOperator.class).compare(o1, o2)){
        return -1;
      }
      if (!Arrays.equals(o1.getAggregations(), o2.getAggregations())) return -1;
      if (!Arrays.equals(o1.getCarryovers(), o2.getCarryovers())) return -1;
      if (o1.getTarget() != null ? !o1.getTarget().equals(o2.getTarget()) : o2.getTarget() != null) return -1;
      if (o1.getWithin() != null ? !o1.getWithin().equals(o2.getWithin()) : o2.getWithin() != null) return -1;
      return 0;
    }
  }

  private static class ScanComparator extends LOPComparator<Scan> {
    @Override
    public int compare(Scan o1, Scan o2) {
      if(PlanMerge.getTableName(o1).equals(PlanMerge.getTableName(o2))
            && o1.getSelection().equals(o2.getSelection())){
        return 0;  
      }
      return -1;
    }
  }

  private static class SegmentComparator extends LOPComparator<Segment> {
    @Override
    public int compare(Segment o1, Segment o2) {
      if(0!=comparators.get(SingleInputOperator.class).compare(o1, o2)){
        return -1;
      }
      if(o1.getExprs() != null ? o1.getExprs().equals(o2.getExprs()): o2.getExprs() != null) return -1;
      if(o1.getName() != null? o1.getName().equals(o2.getName()): o2.getName() != null) return -1;
      return 0;
    }
  }
  
  private static class SingleInputComparator extends LOPComparator<SingleInputOperator>{

    @Override
    public int compare(SingleInputOperator o1, SingleInputOperator o2) {
      if(!equals(o1.getInput(), o2.getInput())) return -1;
      return 0;
    }
  }
}
