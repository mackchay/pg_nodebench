package com.haskov.costs;

public record JoinCacheData (
    String joinType,
    String innerNumTuples,
    String outerNumTuples,
    Integer innerConditionCount,
    Integer outerConditionCount
) {

}
