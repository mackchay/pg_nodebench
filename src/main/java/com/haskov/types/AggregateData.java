package com.haskov.types;

public record AggregateData(
        String column,
        String function,
        ReplaceOrAdd replaceOrAdd
) {
}
