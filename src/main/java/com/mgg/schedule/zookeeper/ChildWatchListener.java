package com.mgg.schedule.zookeeper;

import java.util.List;

public interface ChildWatchListener {

    void childChanage(String path, List<String> children);
}
