package com.haskov.types;

import java.util.List;

public record TableBuildResult(String tableName,
                               List<String> sqlScripts) {

}
