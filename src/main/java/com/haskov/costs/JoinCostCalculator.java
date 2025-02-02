package com.haskov.costs;

import com.haskov.nodes.joins.HashJoin;
import com.haskov.types.JoinNodeType;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.function.Function;

import static com.haskov.costs.CostParameters.*;

public class JoinCostCalculator {

    /**
     * Кэш, который хранит диапазон строк для параметров соединения таблиц,
     * для которых уже происходили выччисления диапазона.
     */

    private Map<JoinCacheData, Pair<Long, Long>> cache = new HashMap<>();

    //Nested Loop

    /**
     * Вычисляет стоимость вложенного цикла (Nested Loop Join).
     *
     * @param innerTableName     Название внутренней таблицы.
     * @param outerTableName     Название внешней таблицы.
     * @param innerTableScanCost Стоимость сканирования внутренней таблицы.
     * @param outerTableScanCost Стоимость сканирования внешней таблицы.
     * @param innerSel           Селективность внутренних условий.
     * @param outerSel           Селективность внешних условий.
     * @param startScanCost      Начальная стоимость сканирования.
     * @param innerConditionCount Количество условий соединения для внутренней таблицы.
     * @param outerConditionCount Количество условий соединения для внешней таблицы.
     * @return Общая стоимость выполнения соединения.
     */
    public static double calculateNestedLoopCost(String innerTableName, String outerTableName,
                                                 double innerTableScanCost, double outerTableScanCost,
                                                 double innerSel, double outerSel, double startScanCost,
                                                 int innerConditionCount, int outerConditionCount) {
        double innerNumTuples, outerNumTuples;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName);
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName);
        return getNestedLoopCost(innerNumTuples, outerNumTuples,
                innerTableScanCost, outerTableScanCost,
                innerSel, outerSel, startScanCost, innerConditionCount, outerConditionCount);
    }

    /**
     * Алгоритм вычисления стоимости вложенного цикла (Nested Loop Join).
     *
     * @param innerNumTuples     Количество строк во внутренней таблице.
     * @param outerNumTuples     Количество строк во внешней таблице.
     * @param innerTableScanCost Стоимость сканирования внутренней таблицы.
     * @param outerTableScanCost Стоимость сканирования внешней таблицы.
     * @param innerSel           Селективность внутренних условий.
     * @param outerSel           Селективность внешних условий.
     * @param startScanCost      Начальная стоимость сканирования.
     * @param innerConditionCount Количество условий соединения для внутренней таблицы.
     * @param outerConditionCount Количество условий соединения для внешней таблицы.
     * @return Общая стоимость выполнения соединения.
     */
    public static double getNestedLoopCost(double innerNumTuples, double outerNumTuples,
                                           double innerTableScanCost, double outerTableScanCost,
                                           double innerSel, double outerSel,
                                           double startScanCost,
                                           int innerConditionCount, int outerConditionCount) {
        double startUpCost, runCost;
        innerNumTuples = Math.max(Math.round(innerNumTuples * Math.pow(innerSel, innerConditionCount)), 1);
        outerNumTuples = Math.max(Math.round(outerNumTuples * Math.pow(outerSel, outerConditionCount)), 1);
        startUpCost = 0;
        runCost = (cpuOperatorCost + cpuTupleCost) * innerNumTuples * outerNumTuples +
                innerTableScanCost * outerNumTuples + outerTableScanCost;
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    //Nested Loop Materialized


    /**
     * Вычисляет стоимость узла Materialize (Materialize).
     *
     * @param innerNumTuples      Количество строк во внутренней таблице.
     * @param innerTableScanCost  Стоимость сканирования внутренней таблицы.
     * @return Общая стоимость узла.
     */
    public static double calculateMaterializeCost(double innerNumTuples, double innerTableScanCost) {
        double startUpCost, runCost;
        startUpCost = 0;
        runCost = 2 * cpuOperatorCost * innerNumTuples;
        return (double) Math.round(startUpCost * 100) / 100
                + innerTableScanCost
                + (double) Math.round(runCost * 100) / 100;
    }

    /**
     * Вычисляет стоимость вложенного цикла с узлом Materialize (Nested Loop Join).
     *
     * @param innerTableName     Название внутренней таблицы.
     * @param outerTableName     Название внешней таблицы.
     * @param innerTableScanCost Стоимость сканирования внутренней таблицы.
     * @param outerTableScanCost Стоимость сканирования внешней таблицы.
     * @param innerSel           Селективность внутренних условий.
     * @param outerSel           Селективность внешних условий.
     * @param innerConditionCount Количество условий соединения для внутренней таблицы.
     * @param outerConditionCount Количество условий соединения для внешней таблицы.
     * @return Общая стоимость выполнения соединения.
     */
    public static double calculateMaterializedNestedLoopCost(String innerTableName, String outerTableName,
                                                             double innerTableScanCost, double outerTableScanCost,
                                                             double innerSel, double outerSel,
                                                             int innerConditionCount, int outerConditionCount) {
        double innerNumTuples, outerNumTuples;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName);
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName);
        return getMaterializedNestedLoopCost(innerNumTuples, outerNumTuples,
                innerTableScanCost, outerTableScanCost,
                innerSel, outerSel,
                innerConditionCount, outerConditionCount);
    }

    /**
     * Алгоритм вычисления стоимости вложенного цикла с узлом Materialize (Nested Loop Join).
     *
     * @param innerNumTuples     Количество строк во внутренней таблице.
     * @param outerNumTuples     Количество строк во внешней таблице.
     * @param innerTableScanCost Стоимость сканирования внутренней таблицы.
     * @param outerTableScanCost Стоимость сканирования внешней таблицы.
     * @param innerSel           Селективность внутренних условий.
     * @param outerSel           Селективность внешних условий.
     * @param innerConditionCount Количество условий соединения для внутренней таблицы.
     * @param outerConditionCount Количество условий соединения для внешней таблицы.
     * @return Общая стоимость выполнения соединения.
     */
    public static double getMaterializedNestedLoopCost(double innerNumTuples, double outerNumTuples,
                                                       double innerTableScanCost, double outerTableScanCost,
                                                       double innerSel, double outerSel,
                                                       int innerConditionCount, int outerConditionCount) {
        double startUpCost, runCost, rescanCost;
        startUpCost = 0;
        innerNumTuples = Math.max(Math.round(innerNumTuples * Math.pow(innerSel, innerConditionCount)), 1);
        outerNumTuples = Math.max(Math.round(outerNumTuples * Math.pow(outerSel, outerConditionCount)), 1);

        rescanCost = cpuOperatorCost * innerNumTuples;
        runCost = (cpuTupleCost + cpuOperatorCost) * innerNumTuples * outerNumTuples +
                rescanCost * (outerNumTuples - 1) + outerTableScanCost + calculateMaterializeCost(innerNumTuples,
                innerTableScanCost);
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    //TODO add index nested loop
    //Index Nested Loop

    public static double calculateIndexedNestedLoopJoinCost() {
        return 0L;
    }

    // Hash Join

    /**
     * Вычисляет стоимость соединения хешированием (Hash Join).
     *
     * @param innerTableName     Название внутренней таблицы.
     * @param outerTableName     Название внешней таблицы.
     * @param innerTableScanCost Стоимость сканирования внутренней таблицы.
     * @param outerTableScanCost Стоимость сканирования внешней таблицы.
     * @param innerSel           Селективность внутренних условий.
     * @param outerSel           Селективность внешних условий.
     * @param startScanCost      Начальная стоимость сканирования.
     * @param innerConditionCount Количество условий соединения для внутренней таблицы.
     * @param outerConditionCount Количество условий соединения для внешней таблицы.
     * @return Общая стоимость выполнения соединения.
     */
    public static double calculateHashJoinCost(String innerTableName, String outerTableName,
                                               double innerTableScanCost, double outerTableScanCost,
                                               double innerSel, double outerSel, double startScanCost,
                                               int innerConditionCount, int outerConditionCount) {
        double innerNumTuples, outerNumTuples;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName);
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName);
        return getHashJoinCost(innerNumTuples, outerNumTuples, innerTableScanCost, outerTableScanCost,
                innerSel, outerSel,
                startScanCost, innerConditionCount, outerConditionCount);
    }

    /**
     * Алгоритм вычисления стоимости соединения хешированием (Hash Join).
     *
     * @param innerNumTuples     Количество строк во внутренней таблице.
     * @param outerNumTuples     Количество строк во внешней таблице.
     * @param innerTableScanCost Стоимость сканирования внутренней таблицы.
     * @param outerTableScanCost Стоимость сканирования внешней таблицы.
     * @param innerSel           Селективность внутренних условий.
     * @param outerSel           Селективность внешних условий.
     * @param startScanCost      Начальная стоимость сканирования.
     * @param innerConditionCount Количество условий соединения для внутренней таблицы.
     * @param outerConditionCount Количество условий соединения для внешней таблицы.
     * @return Общая стоимость выполнения соединения.
     */
    public static double getHashJoinCost(double innerNumTuples, double outerNumTuples,
                                         double innerTableScanCost, double outerTableScanCost,
                                         double innerSel, double outerSel,
                                         double startScanCost,
                                         int innerConditionCount, int outerConditionCount) {
        double startUpCost, runCost,
                hashFunInnerCost, hashFunOuterCost, insertTupleCost,
                resultTupleCost, rescanCost;


        innerNumTuples = Math.max(Math.round(innerNumTuples * Math.pow(innerSel, innerConditionCount)), 1);
        outerNumTuples = Math.max(Math.round(outerNumTuples * Math.pow(outerSel, outerConditionCount)), 1);

        hashFunInnerCost = cpuOperatorCost * innerNumTuples;
        insertTupleCost = cpuTupleCost * innerNumTuples;
        startUpCost = innerTableScanCost + hashFunInnerCost + insertTupleCost + startScanCost;


        hashFunOuterCost = cpuOperatorCost * outerNumTuples;

        //TODO fix rescanCost
        double resultTuples = Math.max((long)(innerNumTuples * Math.pow(outerSel, outerConditionCount)), 1);
        rescanCost = cpuOperatorCost * (innerNumTuples + outerNumTuples) * 0.150112;
        //rescanCost = 0;

        resultTupleCost = cpuTupleCost * resultTuples;
        runCost = outerTableScanCost + hashFunOuterCost + rescanCost +
                resultTupleCost;
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }

    // Merge Join

    /**
     * Вычисляет стоимость соединения слиянием (Merge Join).
     *
     * @param innerTableName     Название внутренней таблицы.
     * @param outerTableName     Название внешней таблицы.
     * @param innerTableScanCost Стоимость сканирования внутренней таблицы.
     * @param outerTableScanCost Стоимость сканирования внешней таблицы.
     * @param innerSel           Селективность внутренних условий.
     * @param outerSel           Селективность внешних условий.
     * @param innerConditionCount Количество условий соединения для внутренней таблицы.
     * @param outerConditionCount Количество условий соединения для внешней таблицы.
     * @return Общая стоимость выполнения соединения.
     */
    public static double calculateMergeJoinCost(String innerTableName, String outerTableName,
                                            double innerTableScanCost, double outerTableScanCost,
                                                double innerSel, double outerSel,
                                            int innerConditionCount, int outerConditionCount) {
        double innerNumTuples, outerNumTuples;
        innerNumTuples = SQLUtils.getTableRowCount(innerTableName);
        outerNumTuples = SQLUtils.getTableRowCount(outerTableName);
        return getMergeJoinCost(innerNumTuples, outerNumTuples, innerTableScanCost, outerTableScanCost,
                innerSel, outerSel,
                innerConditionCount, outerConditionCount);
    }

    /**
     * Алгоритм вычисления стоимости соединения слиянием (Merge Join).
     *
     * @param innerNumTuples     Количество строк во внутренней таблице.
     * @param outerNumTuples     Количество строк во внешней таблице.
     * @param innerTableScanCost Стоимость сканирования внутренней таблицы.
     * @param outerTableScanCost Стоимость сканирования внешней таблицы.
     * @param innerSel           Селективность внутренних условий.
     * @param outerSel           Селективность внешних условий.
     * @param innerConditionCount Количество условий соединения для внутренней таблицы.
     * @param outerConditionCount Количество условий соединения для внешней таблицы.
     * @return Общая стоимость выполнения соединения.
     */
    public static double getMergeJoinCost(double innerNumTuples, double outerNumTuples,
                                          double innerTableScanCost, double outerTableScanCost,
                                          double innerSel, double outerSel,
                                          int innerConditionCount, int outerConditionCount) {
        double startUpCost, runCost, innerResSel = innerSel, outerResSel = outerSel;
        startUpCost = 0;
        innerNumTuples = Math.max(Math.round(innerNumTuples * Math.pow(innerSel, innerConditionCount)), 1);
        outerNumTuples = Math.max(Math.round(outerNumTuples * Math.pow(outerSel, outerConditionCount)), 1);

        double resultTuples = (innerNumTuples / innerSel) * innerResSel * outerResSel;
        runCost = innerTableScanCost + outerTableScanCost + cpuTupleCost *
                (innerNumTuples / innerSel) * innerResSel * outerResSel
                + cpuOperatorCost * (innerNumTuples + outerNumTuples);
        return (double) Math.round(startUpCost * 100) / 100 + (double) Math.round(runCost * 100) / 100;
    }


    /**
     * Вычисляет диапазон кортежей для соединения таблиц.
     *
     * @param innerTableName     Название внутренней таблицы.
     * @param outerTableName     Название внешней таблицы.
     * @param innerCostFunction  Функция, которая вычисляет стоимость внутреннего узла, в зависимости от селективности
     * @param outerCostFunction  Функция, которая вычисляет стоимость внешнего узла, в зависимости от селективности
     * @param minTuples          Ограничение на минимальное количество строк.
     * @param maxTuples          Ограничение на максимльное количество строк.
     * @param innerConditionCount Количество условий соединения для внутренней таблицы.
     * @param outerConditionCount Количество условий соединения для внешней таблицы.
     * @param type               Тип соединения (Hash Join, Merge Join, etc..).
     * @return Пара значений (начальный диапазон, конечный диапазон).
     */
    public Pair<Long, Long> calculateTuplesRange(
            String innerTableName, String outerTableName,
            Function<Double, Pair<Double, Double>> innerCostFunction,
            Function<Double, Pair<Double, Double>> outerCostFunction,
            long minTuples,
            long maxTuples,
            int innerConditionCount,
            int outerConditionCount, JoinNodeType type) {

        JoinCacheData data = new JoinCacheData(
                type.toString(), innerTableName, outerTableName,
                innerConditionCount, outerConditionCount);

        if (cache.containsKey(data)) {
            return cache.get(data);
        }

        double innerNumTuples = SQLUtils.getTableRowCount(innerTableName);
        double outerNumTuples = SQLUtils.getTableRowCount(outerTableName);

        boolean isIndexed = SQLUtils.hasIndexOnTable(innerTableName) && SQLUtils.hasIndexOnTable(outerTableName);

        List<Pair<JoinNodeType, Long>> rangeList = new ArrayList<>();
        if (minTuples < 2) {
            rangeList.add(new ImmutablePair<>(JoinNodeType.NESTED_LOOP, 1L));
        }

        for (long i = Math.max(minTuples, 2); i <= maxTuples; i++) {
            double sel = (double) i / innerNumTuples;

            double innerTableScanCost = innerCostFunction.apply(sel).getRight();
            double outerTableScanCost = outerCostFunction.apply(sel).getRight();
            double startScanCost = Math.max(innerCostFunction.apply(sel).getLeft(),
                    outerCostFunction.apply(sel).getLeft());

            List<JoinNodeType> types = new ArrayList<>(List.of(
                    JoinNodeType.NESTED_LOOP,
                    JoinNodeType.NESTED_LOOP_MATERIALIZED,
                    JoinNodeType.HASH_JOIN,
                    JoinNodeType.MERGE_JOIN));

            List<Double> costs = List.of(
                    getNestedLoopCost(innerNumTuples, outerNumTuples,
                            innerTableScanCost,
                            outerTableScanCost,
                            sel,
                            sel, startScanCost, innerConditionCount, outerConditionCount),
                    getMaterializedNestedLoopCost(innerNumTuples, outerNumTuples, innerTableScanCost,
                            outerTableScanCost, sel, sel, innerConditionCount, outerConditionCount),
                    getHashJoinCost(innerNumTuples, outerNumTuples, innerTableScanCost, outerTableScanCost, sel,
                            sel, startScanCost, innerConditionCount, outerConditionCount),
                    getMergeJoinCost(innerNumTuples, outerNumTuples, innerTableScanCost, outerTableScanCost,
                            sel, sel, innerConditionCount, outerConditionCount)
            );

            // Мы требуем необходимость наличия индексов в обоих таблицах для Merge Join.
            if (!isIndexed) {
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

            if (innerNumTuples * Math.pow(sel, Math.max(innerConditionCount, outerConditionCount)) < 1.5) {
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
