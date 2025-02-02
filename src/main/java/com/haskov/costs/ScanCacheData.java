package com.haskov.costs;

import com.haskov.types.ScanNodeType;

import java.util.Objects;

public record ScanCacheData(
        Integer idxConditions,
        Integer conditions,
        ScanNodeType scanType,
        Double pages,
        Double tuples
) {

}
