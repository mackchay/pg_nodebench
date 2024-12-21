package com.haskov.costs;

import java.util.Objects;

public record ScanCacheData(
        Integer idxConditions,
        Integer conditions,
        String scanType,
        Double pages,
        Double tuples
) {

}
