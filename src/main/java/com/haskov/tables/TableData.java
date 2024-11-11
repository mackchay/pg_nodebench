package com.haskov.tables;

import lombok.Getter;
import lombok.NonNull;

import java.util.List;

@Getter
public class TableData {
    private final String tableName;
    private final List<String> parentTables;
    private final int columns;
    private final long size;
    private final List<Boolean> isIndexRequiredList;
    private final List<String> joinTypes;
    private final boolean isPrimaryKeyReq;

    /**
     * Create table with params
     * @param tableName table name
     * @param parentTables parent tables names (to add Outer Keys).
     * @param columns columns count
     * @param size size of tables in rows
     * @param isIndexRequiredList list of booleans for columns: indexed column or not.
     */
    public TableData(@NonNull String tableName,
                     @NonNull  List<String> parentTables,
                     int columns,
                     long size,
                     @NonNull List<Boolean> isIndexRequiredList, List<String> joinTypes, boolean isPrimaryKeyReq) {
        this.tableName = tableName;
        this.parentTables = parentTables;
        this.columns = columns;
        this.size = size;
        this.isIndexRequiredList = isIndexRequiredList;
        this.joinTypes = joinTypes;
        this.isPrimaryKeyReq = isPrimaryKeyReq;
    }
}
