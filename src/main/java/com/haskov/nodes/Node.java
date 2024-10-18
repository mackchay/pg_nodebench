package com.haskov.nodes;

import java.util.List;

public interface Node {

    public String buildQuery(List<String> tables);

    public List<String> prepareTables(Long tableSize);
}
