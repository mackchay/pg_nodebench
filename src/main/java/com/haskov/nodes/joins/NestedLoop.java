package com.haskov.nodes.joins;

import com.haskov.QueryBuilder;
import com.haskov.bench.V2;
import com.haskov.costs.JoinCostCalculator;
import com.haskov.nodes.Node;
import com.haskov.nodes.NodeTreeData;
import com.haskov.tables.TableBuilder;
import com.haskov.types.JoinData;
import com.haskov.types.JoinNodeType;
import com.haskov.types.JoinType;
import com.haskov.types.TableBuildResult;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

public class NestedLoop implements Node, Join {
    private String parentTable;
    private String childTable;
    private List<String> parentColumns;
    private List<String> childColumns;
    private double parentScanCost;
    private double childScanCost;
    private double startUpCost;
    private int childConditionsCount;
    private int parentConditionsCount;
    private double parentTableSel;
    private double childTableSel;
    private boolean isMaterialized;
    private JoinCostCalculator costCalculator = new JoinCostCalculator();

    @Override
    public void initNode(List<String> tables) {
        //Expected 2 tables.
        parentTable = tables.get(1);
        childTable = tables.getFirst();
        Map<String, String> columnsAndTypesParent = V2.getColumnsAndTypes(parentTable);
        parentColumns = new ArrayList<>(columnsAndTypesParent.keySet());
        Map<String, String> columnsAndTypesChild = V2.getColumnsAndTypes(childTable);
        childColumns = new ArrayList<>(columnsAndTypesChild.keySet());
    }


    @Override
    public QueryBuilder buildQuery(QueryBuilder qb) {

        Collections.shuffle(parentColumns);
        Collections.shuffle(childColumns);
        List<String> selectColumns = qb.getSelectColumns();

        qb.join(new JoinData(
                        parentTable,
                        childTable,
                        JoinType.NON_EQUAL,
                        selectColumns.getLast().substring(selectColumns.getLast().indexOf(".")).replace(".", ""),
                        selectColumns.getFirst().substring(selectColumns.getFirst().indexOf(".")).replace(".", "")
                )
        );
        //qb.addRandomWhere(tables.getLast(), joinColumns.getKey());
        //qb.where(tables.getFirst() + "." + joinColumns.getValue() + " < 2");

        return qb;
    }

    @Override
    public TableBuildResult prepareJoinTable(String childName, String parentTable) {
        return new TableBuildResult(
                childName,
                TableBuilder.addForeignKey(
                        childName,
                        parentTable,
                        this.getClass().getSimpleName()
                )
        );
    }

    @Override
    public Pair<Double, Double> getCosts() {
        double cost = JoinCostCalculator.calculateNestedLoopCost(
                parentTable,
                childTable,
                parentScanCost,
                childScanCost,
                parentTableSel,
                childTableSel,
                startUpCost,
                childConditionsCount,
                parentConditionsCount
        );
        return new ImmutablePair<>(0.0, cost);
    }

    @Override
    public void prepareJoinQuery(NodeTreeData parent, NodeTreeData child) {
        this.parentScanCost = parent.getTotalCost();
        this.childScanCost = child.getTotalCost();
        this.startUpCost = child.getStartUpCost();
        this.childConditionsCount = child.getIndexConditions() + child.getNonIndexConditions();
        this.parentConditionsCount = parent.getIndexConditions() + parent.getNonIndexConditions();
        this.parentTableSel = parent.getSel();
        this.childTableSel = child.getSel();
        this.isMaterialized = parent.isMaterialized() || child.isMaterialized();
    }

    @Override
    public Pair<Long, Long> getTuplesRange() {
        JoinNodeType type;
        double innerCost, outerCost;
        String innerTable, outerTable;
        int innerConditions, outerConditions;

        if (parentScanCost >= childScanCost) {
            innerCost = parentScanCost;
            outerCost = childScanCost;
            innerTable = parentTable;
            outerTable = childTable;
            innerConditions = parentConditionsCount;
            outerConditions = childConditionsCount;
        } else {
            innerCost = childScanCost;
            outerCost = parentScanCost;
            innerTable = childTable;
            outerTable = parentTable;
            innerConditions = childConditionsCount;
            outerConditions = parentConditionsCount;
        }

        if (isMaterialized) {
            type = JoinNodeType.NESTED_LOOP_MATERIALIZED;

        } else {
            type = JoinNodeType.NESTED_LOOP;
        }
            Pair<Long, Long> range = costCalculator.calculateTuplesRange(
                    innerTable,
                    outerTable,
                    innerCost,
                    outerCost,
                    startUpCost,
                    innerConditions,
                    outerConditions,
                    type
            );
        return new ImmutablePair<>(range.getLeft(), range.getRight());
    }

    @Override
    public String buildQuery() {
        return "";
    }
}
