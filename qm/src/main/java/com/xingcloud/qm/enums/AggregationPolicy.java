package com.xingcloud.qm.enums;

public enum AggregationPolicy {

  // 1. 和查询结果保持一致, 无须派生出新查询任务
  SAME_AS_QUERY(false, false, AggregationPolicyDisplayed.QUERY,
                "Same as query result and do <<NOT>> make new descriptors."),
  // 2. 生成单天任务,和它保持一致
  SAME_AS_QUERY_EXTEND(false, false, AggregationPolicyDisplayed.QUERY,
                       "Make a single day-interval-descriptor, keep same as it."),
  // 3. 和TOTAL相同, 无须派生出新查询任务
  SAME_AS_TOTAL(false, false, null, "Same as total summary result and do <<NOT>> make new descriptors."),
  // 4. 直接查询, 不可计算得出
  QUERY(false, false, AggregationPolicyDisplayed.QUERY, "Query directly."),
  // 5. 通过累加各项得出, 无须派生出新查询任务
  ACCUMULATION(true, false, AggregationPolicyDisplayed.QUERY, "Sum all queryed data, do <<NOT>> make new descriptors."),
  // 6. 生成无用户群任务, 累加各项结果
  ACCUMULATION_EXTEND(true, true, AggregationPolicyDisplayed.QUERY,
                      "Make total-user-segment descriptors and sum them all."),
  // 7. 通过计算平均值得出, 无须派生出新查询任务
  AVERAGE(true, false, AggregationPolicyDisplayed.AVG, "Average all queried data, do <<NOT>> make new descriptors."),
  // 8. 生成无用户群任务, 平均各项结果
  AVERAGE_EXTEND(true, true, AggregationPolicyDisplayed.AVG,
                 "Make total-user-segment descriptors and average them all.");

  private boolean checkIntersection;

  private boolean extendAggregationPolicy;

  private AggregationPolicyDisplayed displayName;

  private String description;

  private AggregationPolicy(boolean checkIntersection, boolean extendAggregationPolicy,
                            AggregationPolicyDisplayed displayName, String description) {
    this.checkIntersection = checkIntersection;
    this.extendAggregationPolicy = extendAggregationPolicy;
    this.displayName = displayName;
    this.description = description;
  }

  public String getDescription() {
    return description;
  }

  public boolean needCheckIntersection() {
    return checkIntersection;
  }

  public boolean isExtendAggregationPolicy() {
    return extendAggregationPolicy;
  }

  public boolean isNotExtendAggregationPolicy() {
    return !extendAggregationPolicy;
  }

  public AggregationPolicyDisplayed getDisplayName() {
    return displayName;
  }

  public static void main(String[] args) {
    AggregationPolicy[] policies = AggregationPolicy.values();
    for (AggregationPolicy sp : policies) {
      System.out.println("Name=" + sp.name() + ", DisplayName=" + sp.getDisplayName() + ", NeedCheckIntersection=" + sp
        .needCheckIntersection() + ", Description=" + sp.getDescription());
    }
  }

}
