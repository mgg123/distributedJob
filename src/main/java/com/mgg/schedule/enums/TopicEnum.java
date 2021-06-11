package com.mgg.schedule.enums;

import java.util.ArrayList;
import java.util.List;

/**
 * 临时topic
 */
public enum TopicEnum {

    MGG("mgg_topic",7,"mgg", new ArrayList<String>(){
        {
            add("mgg_topic_0");
            add("mgg_topic_1");
            add("mgg_topic_2");
            add("mgg_topic_3");
            add("mgg_topic_4");
            add("mgg_topic_5");
            add("mgg_topic_6");
            add("mgg_topic_7");
        }
    }),
    KGG("kgg_topic",7,"kgg", new ArrayList<String>(){
        {
            add("kgg_topic_0");
            add("kgg_topic_1");
            add("kgg_topic_2");
            add("kgg_topic_3");
            add("kgg_topic_4");
            add("kgg_topic_5");
            add("kgg_topic_6");
            add("kgg_topic_7");
        }
    });

    String topic;

    Integer slotNum;

    String topicType;

    List<String> slot;

    TopicEnum(String mgg_topic, int i, String mgg, List<String> slot) {
        this.topic = mgg_topic;
        this.slotNum = i;
        this.topicType = mgg;
        this.slot = slot;
    }

    public String getTopic() {
        return topic;
    }

    public Integer getSlotNum() {
        return slotNum;
    }

    public String getTopicType() {
        return topicType;
    }

    public List<String> getSlot() {
        return slot;
    }
}
