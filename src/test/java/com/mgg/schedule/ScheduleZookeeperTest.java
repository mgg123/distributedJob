package com.mgg.schedule;

import com.mgg.schedule.zookeeper.ChildListener;
import com.mgg.schedule.zookeeper.ChildWatchListener;
import com.mgg.schedule.zookeeper.DataListener;
import com.mgg.schedule.zookeeper.EventType;
import com.mgg.schedule.zookeeper.curator.CuratorZookeeperClient;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
public class ScheduleZookeeperTest {

    CuratorZookeeperClient client;

    @Before
    public void testZk() {
        client = new CuratorZookeeperClient("127.0.0.1:2181");
    }


    @Test
    public void test() throws InterruptedException {
        String path = "/mgg/127.0.0.1:8080/slot";
        //client.create(path,true);
        client.addDataListener("/mgg", new DataListener() {
            @Override
            public void dataChanged(String path, Object value, EventType eventType) {
                System.out.println("" + value + "__" + eventType.name());
            }
        });

        client.addChildListener("/mgg", new ChildListener() {
            @Override
            public void childChanged(String path, String data, PathChildrenCacheEvent.Type eventType) {
                System.out.println(path + "_________" + data);
            }
        });

        client.addChildWatchListener("/mgg", new ChildWatchListener() {
            @Override
            public void childChanage(String path, List<String> children) {
                children.forEach(a -> System.out.println(path + "______watcher______" + a));
            }
        });

        while (1==1) {
            Thread.sleep(1000);
        }
    }

    @Test
    public void deletePath() throws InterruptedException {
        client.delete("/mgg");

        while (1==1) {
            Thread.sleep(1000);
        }
    }


}
