package com.mgg.distributedJob.util;

public class KeyUtil {

    /**
     * 获取所在队列对应槽位
     * @param jobid
     * @param topic
     * @param slotNum
     * @return
     */
    public static String getTopicKey(Long jobid,String topic,Integer slotNum) {
        return String.format("%s_%s",topic,jobid.hashCode() & slotNum);
    }
}
