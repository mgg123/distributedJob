package com.mgg.distributedJob.zookeeper;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

public interface ChildListener {

    void childChanged(String path, String data, PathChildrenCacheEvent.Type eventType);

}
