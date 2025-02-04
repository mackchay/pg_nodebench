package com.haskov.costs.scan;

import com.haskov.costs.ScanCacheData;
import com.haskov.types.ScanNodeType;
import lombok.AccessLevel;
import lombok.Getter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;

import static com.haskov.utils.SQLUtils.*;
import static com.haskov.utils.SQLUtils.getVisiblePages;

@Getter
public class TupleRangeCalculator {
    private final SeqScanCostCalculator seqScanCalculator;
    private final IndexOnlyScanCostCalculator indexOnlyCalculator;
    private final IndexScanCostCalculator indexCalculator;
    private final BitmapScanCostCalculator bitmapCalculator;

    @Getter(AccessLevel.NONE)
    private final Map<ScanCacheData, Pair<Long, Long>> cacheMapTuples = new HashMap<>();

    @Getter(AccessLevel.NONE)
    private final long numTuples;


    public TupleRangeCalculator(String tableName, String indexedColumn) {
        Pair<Long, Long> result = getTablePagesAndRowsCount(tableName);
        long numPages = result.getLeft();
        long numTuples = result.getRight();

        this.numTuples = numTuples;

        Pair<Long, Long> indexResult = getTablePagesAndRowsCount(getIndexOnColumn(tableName, indexedColumn));
        long numIndexPages = indexResult.getLeft();
        long numIndexTuples = indexResult.getRight();

        long heightBTree = getBtreeHeight(getIndexOnColumn(tableName, indexedColumn));
        double correlation = getCorrelation(tableName, indexedColumn);
        long visiblePages = getVisiblePages(tableName);

        seqScanCalculator = new SeqScanCostCalculator(numPages, numTuples);
        indexOnlyCalculator = new IndexOnlyScanCostCalculator(numPages, numTuples, numIndexPages, numIndexTuples,
                heightBTree, correlation, visiblePages);
        indexCalculator = new IndexScanCostCalculator(numPages, numTuples, numIndexPages, numIndexTuples,
                heightBTree, correlation);
        bitmapCalculator = new BitmapScanCostCalculator(numPages, numTuples, numIndexPages, numIndexTuples,
                heightBTree);
    }

    public Pair<Long, Long> calculateTuplesRange(int indexConditionsCount, int conditionsCount,
                                                 ScanNodeType type) {

        ScanCacheData data = new ScanCacheData(type, indexConditionsCount, conditionsCount);
        if (cacheMapTuples.containsKey(data)) {
            return new ImmutablePair<>(cacheMapTuples.get(data).getLeft(), cacheMapTuples.get(data).getRight());
        }

        List<Pair<ScanNodeType, Long>> rangeList = new ArrayList<>();

        rangeList.add(new ImmutablePair<>(ScanNodeType.INDEX_SCAN, 1L));

        for (int i = 2; i <= numTuples; i++) {
            double sel = (double) i / numTuples;
            List<ScanNodeType> types = new ArrayList<>(List.of(
                    ScanNodeType.SEQ_SCAN,
                    ScanNodeType.INDEX_SCAN,
                    ScanNodeType.INDEX_ONLY_SCAN,
                    ScanNodeType.BITMAP_SCAN)
            );
            List<Double> costs = new ArrayList<>(List.of(
                    seqScanCalculator.calculateCost(conditionsCount),
                    indexCalculator.calculateCost(indexConditionsCount, conditionsCount, sel),
                    indexOnlyCalculator.calculateCost(indexConditionsCount, sel),
                    bitmapCalculator.calculateCost(indexConditionsCount, conditionsCount, sel)
            ));

            if (!type.equals(ScanNodeType.INDEX_ONLY_SCAN)) {
                costs.remove(types.indexOf(ScanNodeType.INDEX_ONLY_SCAN));
                types.remove(ScanNodeType.INDEX_ONLY_SCAN);
            }

            ScanNodeType bestType = types.stream()
                    .min(Comparator.comparingDouble(t -> costs.get(types.indexOf(t))))
                    .orElse(ScanNodeType.SEQ_SCAN);

            rangeList.add(new ImmutablePair<>(bestType, (long) i));
        }

        List<Long> costList = rangeList.stream()
                .filter(pair -> pair.getKey().equals(type))
                .map(Pair::getValue)
                .toList();

        double error = 1.1;
        Pair<Long, Long> range = new ImmutablePair<>((Math.round(costList.getFirst() * error)),
                (Math.round(costList.getLast() / error)));
        cacheMapTuples.put(data, range);
        return range;
    }
}
