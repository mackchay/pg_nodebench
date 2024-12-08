package com.haskov.nodes.joins;


import com.haskov.types.TableBuildResult;

public interface Join {
    public TableBuildResult prepareJoinTable(String childName, String parentTable);
}
