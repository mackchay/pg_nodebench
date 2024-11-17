package com.haskov.types;
import lombok.NonNull;

import java.util.List;

public record TableData(@NonNull String tableName, @NonNull List<String> parentTables, int columns, long size,
                        @NonNull List<Boolean> isIndexRequiredList, @NonNull List<String> joinTypes,
                        boolean isPrimaryKeyReq) {

}
