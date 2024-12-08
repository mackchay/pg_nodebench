package com.haskov.nodes.scans;

import com.haskov.types.TableBuildResult;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

public interface Scan {
    public TableBuildResult createTable(Long tableSize);
}
