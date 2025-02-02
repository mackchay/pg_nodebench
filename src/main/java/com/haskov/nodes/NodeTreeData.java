package com.haskov.nodes;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class NodeTreeData {
    private Node node;
    private double startUpCost;
    private double totalCost;
    private long minTuples;
    private long maxTuples;
    private int nonIndexConditions;
    private int indexConditions;
    private double sel;
    private boolean isMaterialized;


    public NodeTreeData(Node node, double startUpCost, double totalCost,
                        long minTuples, long maxTuples,
                        int nonIndexConditions, int indexConditions,
                        double sel, boolean isMaterialized) {
        this.node = node;
        this.startUpCost = startUpCost;
        this.totalCost = totalCost;
        this.minTuples = minTuples;
        this.maxTuples = maxTuples;
        this.nonIndexConditions = nonIndexConditions;
        this.indexConditions = indexConditions;
        this.sel = sel;
        this.isMaterialized = isMaterialized;
    }
}
