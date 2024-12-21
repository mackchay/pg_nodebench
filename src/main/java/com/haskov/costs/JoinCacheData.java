package com.haskov.costs;

import java.util.Objects;

public record JoinCacheData (
    String joinType,
    Double innerTableScanCost,
    Double outerTableScanCost,
    Double startScanCost,
    Integer innerConditionCount,
    Integer outerConditionCount
) {

}
