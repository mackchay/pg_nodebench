package com.haskov.costs;

import com.haskov.utils.SQLUtils;

import java.util.Map;


public class CacheParameters {
    private static final Map<String, String> paramsCache = SQLUtils.getCacheParameters();
    static final double sharedBuffers = Double.parseDouble(paramsCache.get("shared_buffers"));
    static final double workMem = Double.parseDouble(paramsCache.get("work_mem"));
    static final double maintenance_work_mem = Double.parseDouble(paramsCache.get("maintenance_work_mem"));
    static final double effective_cache_size = Double.parseDouble(paramsCache.get("effective_cache_size"));
}
