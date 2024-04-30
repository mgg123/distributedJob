package com.mgg.distributedJob.topic;

import cn.hutool.json.JSONUtil;
import com.mgg.distributedJob.enums.TopicEnum;
import com.mgg.distributedJob.util.ConcurrentHashSet;
import com.mgg.distributedJob.zookeeper.ChildListener;
import com.mgg.distributedJob.zookeeper.curator.CuratorZookeeperClient;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.DependsOn;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * 整体topic的管理
 */
@Configuration(value = "topicManager")
@ConditionalOnBean(CuratorZookeeperClient.class)
@DependsOn(value = {"curatorZookeeperClient"})
public class TopicManager {

    /**
     * topic -> slotKeys
     */
    ConcurrentHashMap<String,ConcurrentHashSet<String>> topicSlotKeyMap = new ConcurrentHashMap<>();

    /**
     * key -> topic
     * value -> ips
     */
    ConcurrentHashMap<String, ConcurrentHashSet<String>> topicIpMap = new ConcurrentHashMap<>();


    /**
     * key -> Ip
     * value -> workIds
     */
    ConcurrentHashMap<String, ConcurrentHashSet<String>> ipWorkIdsMap = new ConcurrentHashMap<>();


    /**
     * key -> workId
     * value -> slotkeys
     */
    ConcurrentHashMap<String, ConcurrentHashSet<String>> workIdSlotKeysMap = new ConcurrentHashMap<>();


    /**
     * 工作中workId
     * key -> topic
     * value -> {ip,workId}
     */
    ConcurrentHashMap<String, ConcurrentHashMap<String, ConcurrentHashSet<String>>> workingSlotKey = new ConcurrentHashMap<>();


    @Autowired
    CuratorZookeeperClient curatorZookeeperClient;

    @PostConstruct
    public void postConstruct() {
        topicInit();
        TopicEnum topicEnum = TopicEnum.MGG;
        //创建topic 临时节点
        String path = String.format("/%s",topicEnum.getTopic());
        curatorZookeeperClient.create(path,false);
        curatorZookeeperClient.addChildListener(path,new topicIpChange());
    }



    private void topicInit() {
        for (TopicEnum topicEnum : TopicEnum.values()) {
            ConcurrentHashSet concurrentHashSet = topicSlotKeyMap.getOrDefault(String.format("/%s",topicEnum.getTopic()),new ConcurrentHashSet<>());
            for (int i = 0; i < topicEnum.getSlotNum(); i++) {
                concurrentHashSet.add(topicEnum.getSlot().get(i));
            }
            topicSlotKeyMap.put(String.format("/%s",topicEnum.getTopic()),concurrentHashSet);
        }
    }





    //分配槽位
    public void allocatedSlot(String topic) {
        //获取对应topic下总的slotKeys
        ConcurrentHashSet<String> topicTotalSlotKeys = topicSlotKeyMap.get(topic);
        //获取要分配的机器
        ConcurrentHashSet<String> ips = topicIpMap.get(topic);
        //为workId进行分配
        ConcurrentHashSet<String> topicAllWorkIds = new ConcurrentHashSet<>();
        for (String ip : ips) {
            ConcurrentHashSet<String> workIds =  ipWorkIdsMap.get(ip);
            topicAllWorkIds.addAll(workIds);
        }

        //将slotkeys均匀分配到topicAllWorkIds上
        // 初始化每个 workId 的 slot 分配列表
        for (String workId : topicAllWorkIds) {
            workIdSlotKeysMap.put(workId, new ConcurrentHashSet<>());
        }

        // 轮询分配 slotKeys 到每个 workId
        int index = 0;
        List<String> workIdsList = new ArrayList<>(topicAllWorkIds);
        for (String slotKey : topicTotalSlotKeys) {
            String workId = workIdsList.get(index % workIdsList.size());
            workIdSlotKeysMap.get(workId).add(slotKey);
            index++;
        }

        //workId临时节点内容
        for(Map.Entry<String,ConcurrentHashSet<String>> entry : workIdSlotKeysMap.entrySet()) {
           ConcurrentHashSet<String> workIdSlotKeys = entry.getValue();
           List<String> slotKeys = workIdSlotKeys.stream().collect(Collectors.toList());
           curatorZookeeperClient.createEphemeral(entry.getKey(), JSONUtil.toJsonStr(slotKeys));
        }

    }



    private class topicIpChange implements ChildListener {

        @Override
        public void childChanged(String path, String data, PathChildrenCacheEvent.Type eventType) {
            //机器的节点新增、停机，都会触发当前topic下的slotKey rehash;
            //获取未分配的slotKey进行分配。目前第一版不做再分配（从已分配的slotKey重新hash后再分配）
            if (eventType.equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                //命中的topic; path = /topic/{ip}
                String topic = path.substring(0,path.lastIndexOf("/"));
                //获取workIp;
                ConcurrentHashSet<String> workIps;
                if(topicIpMap.containsKey(topic)) {
                    workIps = topicIpMap.get(topic);
                } else {
                    workIps = new ConcurrentHashSet<>();
                    topicIpMap.put(topic,workIps);
                    curatorZookeeperClient.addChildListener(path,new workIdChange());
                }
                //添加新上报的workIp;
                workIps.add(path);
            } else if(eventType.equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                //path = /topic/{ip} -> {workIds} -> {slotKeys}


            } else if(eventType.equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                //path = /

            }
        }
    }


    private class workIdChange implements ChildListener {

        @Override
        public void childChanged(String path, String data, PathChildrenCacheEvent.Type eventType) {
            //path -> /topic/workIp/workId

            if (eventType.equals(PathChildrenCacheEvent.Type.CHILD_ADDED)) {
                //命中的topic; path = /topic/{ip}/workId
                String topic = path.substring(0,path.lastIndexOf("/",1));
                String workIp = path.substring(0,path.lastIndexOf("/"));
                ConcurrentHashSet<String> ipWorkIds;
                if(ipWorkIdsMap.containsKey(workIp)) {
                    ipWorkIds = ipWorkIdsMap.get(workIp);
                } else {
                    ipWorkIds = new ConcurrentHashSet<>();
                    ipWorkIdsMap.put(workIp,ipWorkIds);
                }
                //workIds -> thread_1, thread_2
                ipWorkIds.add(path);
            } else if(eventType.equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)) {
                //path = /topic/{ip} -> {workIds} -> {slotKeys}


            } else if(eventType.equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)) {
                //path = /

            }

        }
    }




    public static void main(String[] args) {
        String path = "/mgg_topic/127.0.0.1:8080";
        String topic = path.substring(0,path.lastIndexOf("/"));
        System.out.println(topic);
    }


}
