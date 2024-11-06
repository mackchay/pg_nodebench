package com.haskov.costs;

import com.haskov.utils.SQLUtils;
import lombok.Getter;

import java.util.Map;

@Getter
public class CostParameters {
    static final Double seqPageCost = Double.parseDouble(SQLUtils.getCostParameters().get("seq_page_cost"));
    static final Double randomPageCost = Double.parseDouble(SQLUtils.getCostParameters().get("random_page_cost"));
    static final Double cpuTupleCost = Double.parseDouble(SQLUtils.getCostParameters().get("cpu_tuple_cost"));
    static final Double cpuIndexTupleCost = Double.parseDouble(SQLUtils.getCostParameters().get("cpu_index_tuple_cost"));
    static final Double cpuOperatorCost = Double.parseDouble(SQLUtils.getCostParameters().get("cpu_operator_cost"));

}
