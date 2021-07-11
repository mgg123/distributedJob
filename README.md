# distributedJob

基于redis-zookeeper的分布式超时(定时)任务执行

在redis中设计2个Queue;StoreQueue与PrepareQueue。多出来的Queue为了保障消息至少能够消费一次。


