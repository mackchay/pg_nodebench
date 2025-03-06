package com.haskov.types;

public record JoinData(String parentTable,
                       String childTable,
                       JoinType joinType,
                       String parentColumn,
                       String childColumn) {

}
