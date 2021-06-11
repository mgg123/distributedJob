package com.mgg.schedule.zookeeper;


public interface DataListener {

    void dataChanged(String path, Object value, EventType eventType);
}
