package com.xingcluod.qm.utils;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.LogicalPlan;
import org.apache.drill.common.util.FileUtils;
import org.junit.Test;

import java.io.IOException;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/10/13
 * Time: 11:22 AM
 * To change this template use File | Settings | File Templates.
 */
public class TestLogicalPlanRead {
    @Test
    public void readFilter() throws IOException {
        String filterPlan="/plans/filter.json";
        LogicalPlan plan=LogicalPlan.parse(DrillConfig.create(), FileUtils.getResourceAsString(filterPlan));
    }
}
