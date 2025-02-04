package com.haskov.costs.scan;

import com.haskov.costs.JoinCacheData;
import com.haskov.types.JoinNodeType;
import com.haskov.utils.SQLUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.*;
import java.util.function.BiPredicate;
import java.util.function.Function;

import static com.haskov.costs.CostParameters.*;

public class JoinCostCalculator {

    public static long[] findSubarray(long A, long B, long otherA, long otherB,
                                     BiPredicate<Long, Long> condition) {
        // Создаем map: каждому x ставим в соответствие все y, с которыми он образует допустимую пару
        Map<Long, List<Long>> validPairs = new HashMap<>();
        for (long x = A; x <= B; x++) {
            List<Long> validYs = new ArrayList<>();
            for (long y = otherA; y <= otherB; y++) {
                if (condition.test(x, y)) {
                    validYs.add(y);
                }
            }
            if (!validYs.isEmpty()) {
                validPairs.put(x, validYs);
            }
        }

        // Двигаем правую границу, пока сохраняется допустимость
        long maxL = A, maxR = A, bestSize = 0;
        long L = A, R = A;
        while (L <= B) {
            while (R <= B && isValidSubarray(validPairs, L, R)) {
                if ((R - L + 1) > bestSize) {
                    maxL = L;
                    maxR = R;
                    bestSize = R - L + 1;
                }
                R++;
            }
            L++;
        }
        return new long[]{maxL, maxR};
    }

    // Проверяет, можно ли сформировать пары от L до R
    private static boolean isValidSubarray(Map<Long, List<Long>> validPairs, long L, long R) {
        for (long x = L; x <= R; x++) {
            if (!validPairs.containsKey(x) || validPairs.get(x).isEmpty()) {
                return false;
            }
        }
        return true;
    }

}
