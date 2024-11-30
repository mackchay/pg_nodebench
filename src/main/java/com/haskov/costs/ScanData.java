package com.haskov.costs;

import java.util.Objects;

public record ScanData(
        Integer idxConditions,
        Integer conditions,
        String scanType,
        Double pages,
        Double tuples
) {

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScanData scanData = (ScanData) o;
        return Objects.equals(scanType, scanData.scanType) && Objects.equals(conditions, scanData.conditions) && Objects.equals(idxConditions, scanData.idxConditions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(idxConditions, conditions, scanType, pages, tuples);
    }
}
