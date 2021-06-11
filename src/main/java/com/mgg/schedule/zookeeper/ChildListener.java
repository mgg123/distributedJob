package com.mgg.schedule.zookeeper;

import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;

import java.util.List;

public interface ChildListener {

    void childChanged(String path, String data, PathChildrenCacheEvent.Type eventType);

}
