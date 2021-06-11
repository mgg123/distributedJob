package com.mgg.schedule.zookeeper;

import java.util.List;

/**
 * zookeeper 接口
 */
public interface ZookeeperClient {

    void create(String path, boolean ephemeral);

    void create(String path, String content,boolean ephemeral);

    void delete(String path);

    List<String> getChildren(String path);

    void addChildListener(String path, ChildListener listener);

    void addDataListener(String path, DataListener listener);

    void addChildWatchListener(String path, ChildWatchListener listener);

    void removeDataListener(String path, DataListener listener);

    void removeChildListener(String path, ChildListener listener);

    boolean checkExists(String path);

    public String getContent(String path);

}
