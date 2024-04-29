package com.mgg.distributedJob.message;

/**
 * 存储队列的抽象
 */
public class StoreQueue {

    /**
     * 主题
     */
    String topic;

    /**
     * 消息存储划分的槽数量
     */
    Integer slotNum = 8;

    /**
     * topicType
     */
    String topicType;


    public String getTopic() {
        return topic;
    }

    public StoreQueue setTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public Integer getSlotNum() {
        return slotNum;
    }

    public StoreQueue setSlotNum(Integer slotNum) {
        this.slotNum = slotNum;
        return this;
    }

    public String getTopicType() {
        return topicType;
    }

    public StoreQueue setTopicType(String topicType) {
        this.topicType = topicType;
        return this;
    }
}
