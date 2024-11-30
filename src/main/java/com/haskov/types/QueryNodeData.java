package com.haskov.types;


import com.haskov.QueryBuilder;
import lombok.Setter;
import lombok.Getter;

import java.util.List;

@Setter
@Getter
public class QueryNodeData {
        private List<TableBuildResult> tableBuildDataList;
        private QueryBuilder queryBuilder;
        private long tableSize;

        public QueryNodeData(List<TableBuildResult> tableBuildDataList, QueryBuilder queryBuilder, long tableSize) {
                this.tableBuildDataList = tableBuildDataList;
                this.queryBuilder = queryBuilder;
                this.tableSize = tableSize;
        }
}
