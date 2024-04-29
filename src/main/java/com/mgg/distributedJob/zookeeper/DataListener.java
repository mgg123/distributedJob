package com.mgg.distributedJob.zookeeper;


public interface DataListener {

    void dataChanged(String path, Object value, EventType eventType);
}
