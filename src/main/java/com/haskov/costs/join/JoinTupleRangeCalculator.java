package com.haskov.costs.join;

import com.haskov.costs.JoinCacheData;
import com.haskov.types.JoinNodeType;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.function.Function;

import static com.haskov.utils.SQLUtils.getTableRowCount;
import static com.haskov.utils.SQLUtils.hasIndexOnTable;

public class JoinTupleRangeCalculator {
    private Map<JoinCacheData, Pair<Long, Long>> cache = new HashMap<>();

    @Getter
    private final NestedLoopJoinCostCalculator nestedLoopCalculator;
    @Getter
    private final HashJoinCostCalculator hashJoinCalculator;
    @Getter
    private final MergeJoinCostCalculator mergeJoinCalculator;
    private final Function<Double, Pair<Double, Double>> leftCostFunction;
    private final Function<Double, Pair<Double, Double>> rightCostFunction;
    private final boolean isRightTableIndexed;
    private final boolean isLeftTableIndexed;
    private final long leftTuples;
    private final long rightTuples;
    private final JoinNodeType type;


    public JoinTupleRangeCalculator(String leftTable, String rightTable,
                                    Function<Double, Pair<Double, Double>> rightCostFunction,
                                    Function<Double, Pair<Double, Double>> leftCostFunction,
                                    JoinNodeType type) {
        leftTuples = getTableRowCount(leftTable);
        rightTuples = getTableRowCount(rightTable);

        this.nestedLoopCalculator = new NestedLoopJoinCostCalculator(leftTuples, rightTuples);
        this.hashJoinCalculator = new HashJoinCostCalculator(leftTuples, rightTuples);
        this.mergeJoinCalculator = new MergeJoinCostCalculator(leftTuples, rightTuples);

        this.leftCostFunction = rightCostFunction;
        this.rightCostFunction = leftCostFunction;
        isRightTableIndexed = hasIndexOnTable(leftTable);
        isLeftTableIndexed = hasIndexOnTable(rightTable);
        this.type = type;
    }

    /**
     * Вычисляет диапазон кортежей для соединения таблиц.
     *
     * @param minTuples          Ограничение на минимальное количество строк.
     * @param maxTuples          Ограничение на максимльное количество строк.
     * @param leftConditionCount Количество условий соединения для левой таблицы.
     * @param rightConditionCount Количество условий соединения для правой таблицы.
     * @return Пара значений (начальный диапазон, конечный диапазон).
     */

    public Pair<Long, Long> calculateTuplesRange(
            long minTuples,
            long maxTuples,
            int leftConditionCount,
            int rightConditionCount) {

        JoinCacheData data = new JoinCacheData(
                type.toString(),
                leftConditionCount, rightConditionCount);

        if (cache.containsKey(data)) {
            return cache.get(data);
        }


        List<Pair<JoinNodeType, Long>> rangeList = new ArrayList<>();
        if (minTuples < 2) {
            rangeList.add(new ImmutablePair<>(JoinNodeType.NESTED_LOOP, 1L));
        }

        for (long i = Math.max(minTuples, 2); i <= maxTuples; i++) {
            double sel = (double) i / leftTuples;

            double leftTableScanCost = leftCostFunction.apply(sel).getRight();
            double rightTableScanCost = rightCostFunction.apply(sel).getRight();
            double startScanCost = Math.max(leftCostFunction.apply(sel).getLeft(),
                    rightCostFunction.apply(sel).getLeft());

            double bigScanCost, smallScanCost;
            int bigScanConditionCount, smallScanConditionCount;
            if (rightTableScanCost < leftTableScanCost) {
                bigScanCost = leftTableScanCost;
                smallScanCost = rightTableScanCost;
                bigScanConditionCount = leftConditionCount;
                smallScanConditionCount = rightConditionCount;
            } else {
                bigScanCost = rightTableScanCost;
                smallScanCost = leftTableScanCost;
                bigScanConditionCount = rightConditionCount;
                smallScanConditionCount = leftConditionCount;
            }

            List<JoinNodeType> types = new ArrayList<>(List.of(
                    JoinNodeType.NESTED_LOOP,
                    JoinNodeType.NESTED_LOOP_MATERIALIZED,
                    JoinNodeType.HASH_JOIN,
                    JoinNodeType.MERGE_JOIN));

            List<Double> costs = List.of(
                    nestedLoopCalculator.calculateCost(bigScanCost, smallScanCost,
                            sel, sel, bigScanConditionCount, smallScanConditionCount),
                    nestedLoopCalculator.calculateMaterializedCost(bigScanCost,
                            smallScanCost, sel, sel, bigScanConditionCount, smallScanConditionCount),
                    hashJoinCalculator.calculateCost(smallScanCost, bigScanCost, sel,
                            sel, smallScanConditionCount, bigScanConditionCount, startScanCost),
                    mergeJoinCalculator.calculateCost(leftTableScanCost, rightTableScanCost,
                            sel, sel, leftConditionCount, rightConditionCount)
            );

            // Мы требуем необходимость наличия индексов в обоих таблицах для Merge Join.
            if (!isLeftTableIndexed || !isRightTableIndexed) {
                types.remove(JoinNodeType.MERGE_JOIN);
            }

            if (type == JoinNodeType.NESTED_LOOP || type == JoinNodeType.NESTED_LOOP_MATERIALIZED) {
                types.remove(JoinNodeType.HASH_JOIN);
            }

            /*
            Этот код фильтрует неподходящие стратегии соединения. Если прогнозируемое количество строк
            во внутренней таблице после применения условий соединения меньше 1.5, то
            материализованный вложенный цикл не нужен, потому что хранить почти пустую таблицу бессмысленно.
            Хеш-соединение тоже неэффективно, так как хеш-таблица не окупается.
            Самый выгодный вариант Nested Loop без материализации.
            */

            if (leftTuples * Math.pow(sel, Math.max(leftConditionCount, rightConditionCount)) < 1.5) {
                types.remove(JoinNodeType.NESTED_LOOP_MATERIALIZED);
                types.remove(JoinNodeType.HASH_JOIN);
            }


            JoinNodeType bestType = types.stream()
                    .min(Comparator.comparingDouble(t -> costs.get(types.indexOf(t))))
                    .orElse(JoinNodeType.NESTED_LOOP);

            rangeList.add(new ImmutablePair<>(bestType, i));
        }

        List<Long> costList = rangeList.stream()
                .filter(pair -> pair.getKey().equals(type))
                .map(Pair::getValue)
                .toList();

        Pair<Long, Long> range = new ImmutablePair<>(costList.getFirst(), costList.getLast());
        cache.put(data, range);
        return range;
    }



}
