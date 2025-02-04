package com.haskov.costs.scan;

import com.haskov.utils.SQLUtils;

import java.util.Map;


public class CostParameters {
    private static final Map<String, String> paramsCost = SQLUtils.getCostParameters();
    static final Double seqPageCost = Double.parseDouble(paramsCost.get("seq_page_cost"));
    static final Double randomPageCost = Double.parseDouble(paramsCost.get("random_page_cost"));
    static final Double cpuTupleCost = Double.parseDouble(paramsCost.get("cpu_tuple_cost"));
    static final Double cpuIndexTupleCost = Double.parseDouble(paramsCost.get("cpu_index_tuple_cost"));
    static final Double cpuOperatorCost = Double.parseDouble(paramsCost.get("cpu_operator_cost"));
    static final Double qualOpCost = cpuOperatorCost;
}
