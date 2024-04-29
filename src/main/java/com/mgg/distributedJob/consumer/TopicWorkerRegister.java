package com.mgg.distributedJob.consumer;


import cn.hutool.json.JSONUtil;
import com.mgg.distributedJob.enums.TopicEnum;
import com.mgg.distributedJob.message.Work;
import com.mgg.distributedJob.topic.TopicManager;
import com.mgg.distributedJob.zookeeper.ChildListener;
import com.mgg.distributedJob.zookeeper.DataListener;
import com.mgg.distributedJob.zookeeper.EventType;
import com.mgg.distributedJob.zookeeper.curator.CuratorZookeeperClient;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.redisson.Redisson;
import org.redisson.api.RScoredSortedSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Component;
import org.springframework.util.StringUtils;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;

/**
 * 服务实例启动时，work线程与topic进行分配协调
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
@ConditionalOnBean(value = TopicManager.class)
public class TopicWorkerRegister {

    private static final Logger log = LoggerFactory.getLogger(TopicWorkerRegister.class);

    @Autowired
    Redisson redisson;

    //机器ip
    private List<String> workIp = new ArrayList<String>() {
        {
            add("127.0.0.1:8080");
        }
    };


    //线程总数--需可配
    private int workerSum = 4;

    //zookeeper客户端
    @Autowired
    CuratorZookeeperClient curatorZookeeperClient;

    //当前机器workers
    private final CopyOnWriteArrayList<Thread> workers = new CopyOnWriteArrayList<>();

    /**
     * KEY -> workId   /topic/ip/workId
     * VALUE -> thread
     */
    private final ConcurrentMap<String,Runnable> pathWorkMap = new ConcurrentHashMap<>();


    /**
     * key -> runable
     * value -> slotKeys
     */
    private final ConcurrentMap<Runnable,List<String>> workIdSlotKeyMap = new ConcurrentHashMap<>();


    //上报topic worker slot
//    public TopicWorkerRegister() {
//
//        for(TopicEnum topicEnum : TopicEnum.values()) {
//            String path = String.format("/%s",topicEnum.getTopic());
//            curatorZookeeperClient.addChildListener(path,new ChildrenListener());
//            //创建topic 临时节点
//            curatorZookeeperClient.create(String.format("/%s",topicEnum.getTopic()),true);
//            //创建topic slot
//            for(int i = 0; i < topicEnum.getSlotNum(); i++) {
//                curatorZookeeperClient.create(String.format("/%s/%s",topicEnum.getTopic(),i),false);
//            }
//        }
//        String path = String.format("/%s/%s",topicEnum.getTopic(),ip);
//        //创建topic与机器的映射关系
//        curatorZookeeperClient.create(path,false);
//
//        for(TopicEnum topicEnum : TopicEnum.values()) {
//            //假设有两台机器，分别在zookeeper上注册
//        }
//
//        //
//        //publishWorker2Zookeepr();
//    }

    @PostConstruct
    public void postConstruct() {
        publishWorker2Zookeepr();
    }

    private void publishWorker2Zookeepr() {
        //假设每台机器启动4个线程也就是启动4个worker
        for(int i = 0; i < 4; i++) {
            String workerName = String.format("local_thread_%s",i);
            Thread thread = new Thread(new Worker(workerName,TopicEnum.getTopics(),workIp.get(0),curatorZookeeperClient), workerName);
            thread.start();
        }
    }

    private class Worker implements Runnable {

        //work name
        private String name;

        //zookeeper客户端
        private CuratorZookeeperClient client;

        private List<String> topics;

        //工作线程所维护的slotkeys,当机器的重新启动或停止都会重新分配slot,通过监听节点变化来分配slot
        private List<String> workSlotKeys = new ArrayList<>();

        public Worker(String name, List<String> topics, String workIp, CuratorZookeeperClient client) {
            this.name = name;
            this.client = client;
            this.topics = topics;
            for(String topic : topics) {
                String path = String.format("/%s/%s/%s",topic,workIp,name);
                client.createGenericDataListener(path, (path1, value, eventType) -> {
                    if(eventType == EventType.NodeDataChanged) {
                        //slotKey更新。
                        workSlotKeys = JSONUtil.toList(value.toString(),String.class);
                        notifyAll();
                    }
                });
                pathWorkMap.putIfAbsent(path,this);
            }
        }

        @Override
        public void run() {
            for(;;) {
                //如果worker没有分配到slot
                if(workSlotKeys.isEmpty()) {
                    try {
                        Thread.currentThread().wait(300000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                } else {
                    //
                    for(String slot : workSlotKeys) {
                        //获取过期任务
                        RScoredSortedSet<Work> rs = redisson.getScoredSortedSet(slot);
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

    private class SlotRegister implements ChildListener {
        @Override
        public void childChanged(String path, String data, PathChildrenCacheEvent.Type eventType) {

            //若某topic下新增消费者机器、
            if (eventType.equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {

            }


            //


        }


//        @Override
//        public void childChanged(String path, String data, PathChildrenCacheEvent.Type eventType) {
//
//            //监听/#{topic}/ip/路径下有新增，那么唤醒对应worker
//            if (eventType.equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
//                path = path.substring(path.lastIndexOf("/"),path.length());
//                Thread notifyThread = workerBlock.get(path);
//                if (notifyThread != null) {
//                    notifyThread.notifyAll();
//                }
//            }
//
//        }
    }

}
