package com.mgg.distributedJob.config;

import com.mgg.distributedJob.zookeeper.curator.CuratorZookeeperClient;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class CuratorZookeeperConfig {


    @Bean
    public CuratorZookeeperClient getCuratorZookeeperClient()
    {
        return new CuratorZookeeperClient("127.0.0.1:2181");
    }

}
