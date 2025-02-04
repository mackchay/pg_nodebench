package com.haskov.costs;

import com.haskov.types.ScanNodeType;

import java.util.Objects;

public record ScanCacheData(
        ScanNodeType scanType,
        Integer idxConditions,
        Integer conditions
) {

}
