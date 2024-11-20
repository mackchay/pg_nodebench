package com.haskov.types;


import com.haskov.QueryBuilder;
import lombok.Setter;
import lombok.Getter;

import java.util.List;

@Setter
@Getter
public class QueryNodeData {
        private List<String> tables;
        private QueryBuilder queryBuilder;
        private long tableSize;

        public QueryNodeData(List<String> tables, QueryBuilder queryBuilder, long tableSize) {
                this.tables = tables;
                this.queryBuilder = queryBuilder;
                this.tableSize = tableSize;
        }
}
