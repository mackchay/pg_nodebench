package com.haskov.costs;

public record JoinCacheData (
    String joinType,
    Integer innerConditionCount,
    Integer outerConditionCount
) {

}
