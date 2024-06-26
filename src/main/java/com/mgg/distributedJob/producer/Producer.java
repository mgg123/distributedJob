package com.mgg.distributedJob.producer;

import com.mgg.distributedJob.enums.TopicEnum;
import com.mgg.distributedJob.message.Work;
import com.mgg.distributedJob.util.KeyUtil;
import org.redisson.Redisson;
import org.redisson.api.RScoredSortedSet;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 任务生产者
 */
@Component
public class Producer {

    @Autowired
    Redisson redisson;

    public void prowork(List<Work> works) {
        works.forEach(work -> {
            String key = KeyUtil.getTopicKey(work.getId(), TopicEnum.MGG.getTopic(),TopicEnum.MGG.getSlotNum());
            RScoredSortedSet<String> set = redisson.getScoredSortedSet(key);
            set.add(work.getExecuteTime(),work.getId()+"");
        });
    }

}
