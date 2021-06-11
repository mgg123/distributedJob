package com.mgg.schedule.consumer;


import com.mgg.schedule.enums.TopicEnum;
import com.mgg.schedule.message.Work;
import com.mgg.schedule.zookeeper.ChildListener;
import com.mgg.schedule.zookeeper.EventType;
import com.mgg.schedule.zookeeper.curator.CuratorZookeeperClient;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.redisson.Redisson;
import org.redisson.api.RScoredSortedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.security.RunAs;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * work线程与topic进行分配协调
 * zookeeper 维护topic slot--worker之间的联系
 * 数据结构
 *     mgg_topic
 *    /     \
 * 127:8080 127:8081
 *   /          \
 * |127:8080_1|   |127:8081_1|
 * |slot1,slot2|  |slot3,slot4|
 */
@Component
public class TopicWorkerCoordinate {

    private static final Logger log = LoggerFactory.getLogger(TopicWorkerCoordinate.class);

    @Autowired
    Redisson redisson;

    //@Junit
    private List<String> workIp = new ArrayList<String>() {
        {
            add("127.0.0.1:8080");
            add("127.0.0.1:8081");
        }
    };

    private List<String> topics = new ArrayList<String>() {
        {
            add("mgg_topic");
            add("kgg_topic");
        }
    };

    //线程总数--需可配
    private int workerSum = 4;

    //zookeeper客户端
    private final CuratorZookeeperClient curatorZookeeperClient;

    //当前机器workers
    private final CopyOnWriteArrayList<Thread> workers = new CopyOnWriteArrayList<>();

    //worker block
    private final ConcurrentMap<String,Thread> workerBlock = new ConcurrentHashMap<>();

    public TopicWorkerCoordinate() {
        this.curatorZookeeperClient = new CuratorZookeeperClient("127.0.0.1:2181");
        for(TopicEnum topicEnum : TopicEnum.values()) {
            for (String ip : workIp) {
                String path = String.format("/%s/%s",topicEnum.getTopic(),ip);
                curatorZookeeperClient.create(path,false);
                curatorZookeeperClient.addChildListener(path,new ChildrenListener());
            }
        }
        publishWorker2Zookeepr();
    }

    private void publishWorker2Zookeepr() {
        for(int i = 0; i < 4; i++) {
            String workerName = String.format("127.0.0.1:8080_%s",i);
            Thread thread = new Thread(new Worker(workerName,topics,curatorZookeeperClient), workerName);
            workers.add(thread);
            thread.start();
        }
    }

    private class Worker implements Runnable {

        private String name;

        private CuratorZookeeperClient client;

        private List<String> paths;

        public Worker(String name,List<String> topics, CuratorZookeeperClient client) {
            this.name = name;
            this.client = client;
            for(String topic : topics) {
                String path = String.format("/%s/127.0.0.1:8080/%s",topic,name);
                client.create(path,true);
                paths.add(path);
            }
        }

        @Override
        public void run() {
            List<String> slots = new ArrayList<>();
            for(;;) {
                for(String path : paths) {
                   String slot = client.getContent(path);
                   if(!StringUtils.isEmpty(slot)) {
                      slots.add(slot);
                   }
                }

                if(slots.isEmpty()) {
                    workerBlock.putIfAbsent(name,Thread.currentThread());
                    try {
                        Thread.currentThread().wait(300000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }

                //
                for(String slot : slots) {
                    String[] slts = slot.split(",");
                    for(String slt : slts) {
                        log.info(name +"__slot__" +slt);
                        //获取过期任务
                        RScoredSortedSet<Work> rs = redisson.getScoredSortedSet(slt);
                        Collection<Work> works = rs.valueRange(0d,true,Double.valueOf(String.valueOf(System.currentTimeMillis())),false);
                        for(Work work : works) {
                            log.info(work.toString());
                            //业务处理
                        }
                    }
                }
            }
        }
    }

    private class ChildrenListener implements ChildListener {

        @Override
        public void childChanged(String path, String data, PathChildrenCacheEvent.Type eventType) {

            if (eventType.equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                path = path.substring(path.lastIndexOf("/"),path.length());
                Thread notifyThread = workerBlock.get(path);
                if (notifyThread != null) {
                    notifyThread.notifyAll();
                }
            }

        }
    }

}
