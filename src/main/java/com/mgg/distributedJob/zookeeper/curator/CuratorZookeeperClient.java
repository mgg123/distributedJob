package com.mgg.distributedJob.zookeeper.curator;

import com.mgg.distributedJob.zookeeper.ChildListener;
import com.mgg.distributedJob.zookeeper.ChildWatchListener;
import com.mgg.distributedJob.zookeeper.DataListener;
import com.mgg.distributedJob.zookeeper.EventType;
import com.mgg.distributedJob.zookeeper.support.AbstractZookeeperClient;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.api.CuratorWatcher;
import org.apache.curator.framework.recipes.cache.*;
import org.apache.curator.framework.state.ConnectionState;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * zookeeper 客户端
 */
public class CuratorZookeeperClient extends AbstractZookeeperClient<CuratorZookeeperClient.NodeCacheListenerImpl, CuratorZookeeperClient.PathChildCacheListenerImpl, CuratorZookeeperClient.CuratorWatcherImpl> {

    protected static final Logger logger = LoggerFactory.getLogger(CuratorZookeeperClient.class);
    static final Charset CHARSET = StandardCharsets.UTF_8;
    private final CuratorFramework client;
    //当前节点监听
    private static Map<String, NodeCache> nodeCacheMap = new ConcurrentHashMap<>();
    //当前子节点监听
    private static Map<String,PathChildrenCache> childNodeCacheMap = new ConcurrentHashMap<>();

    public CuratorZookeeperClient(String address) {
        super(address);
        client = CuratorFrameworkFactory.builder()
                .connectString("127.0.0.1:2181")
                .sessionTimeoutMs(60000)
                .connectionTimeoutMs(5000)
                .retryPolicy(new ExponentialBackoffRetry(1000,3))
                .build();
        client.getConnectionStateListenable().addListener(new CuratorConnectionStateListener());
        try {
            client.start();
            boolean connected = client.blockUntilConnected(3000, TimeUnit.MILLISECONDS);
            if (!connected) {
                throw  new IllegalAccessException("zookeeper can not connected");
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public String doGetContent(String path) {
        try {
            byte[] dataBytes = client.getData().forPath(path);
            return (dataBytes == null || dataBytes.length == 0) ? null :new String(dataBytes, CHARSET);
        } catch (KeeperException.NoNodeException e) {
            //ignore
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(),e);
        }
        return null;
    }

    @Override
    public void createPersistent(String path) {
        try {
            client.create().forPath(path);
        } catch (KeeperException.NodeExistsException e) {
            logger.warn("ZNode " + path + "already exists ",e);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void createEphemeral(String path) {
        try {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(path);
        } catch (KeeperException.NodeExistsException e) {
            logger.warn("ZNode " + path + " already exists, 该节点可能在会话过期时未来得及删除,所以进行了删除再创建");
            deletePath(path);
            createEphemeral(path);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void createPersistent(String path, String content) {
        byte[] dataBytes = content.getBytes(CHARSET);
        try {
            client.create().forPath(path,dataBytes);
        } catch (KeeperException.NodeExistsException e) {
            try {
                client.setData().forPath(path,dataBytes);
            } catch (Exception e1) {
                throw new IllegalStateException(e1.getMessage(), e1);
            }
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(),e);
        }
    }

    @Override
    public void createEphemeral(String path, String content) {
        byte[] dataBytes = content.getBytes(CHARSET);
        try {
            client.create().withMode(CreateMode.EPHEMERAL).forPath(path, dataBytes);
        } catch (KeeperException.NodeExistsException e) {
            logger.warn("ZNode " + path + " already exists, 该节点可能在会话过期时未来得及删除,所以进行了删除再创建");
            deletePath(path);
            createEphemeral(path, content);
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public void deletePath(String path) {
        try {
            client.delete().deletingChildrenIfNeeded().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            //IGNORE
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public CuratorZookeeperClient.PathChildCacheListenerImpl createGenericChildListener(String path, ChildListener k) {
        return new CuratorZookeeperClient.PathChildCacheListenerImpl(k);
    }

    @Override
    public void addGenericChildListener(String path, CuratorZookeeperClient.PathChildCacheListenerImpl pathChildCacheListener) {
        PathChildrenCache pathChildrenCache = new PathChildrenCache(client,path,true);
        if(childNodeCacheMap.putIfAbsent(path,pathChildrenCache) != null) {
            return ;
        }
        pathChildrenCache.getListenable().addListener(pathChildCacheListener);
        try {
            pathChildrenCache.start();
        } catch (Exception e) {
            throw new IllegalStateException("Add PathChildrenCache listener for path:" + path);
        }
    }

    @Override
    public CuratorWatcherImpl createGenericChildWatchListener(String path, ChildWatchListener k) {
        return new CuratorWatcherImpl(client,k,path);
    }

    @Override
    public List<String> addGenericChildWatchListener(String path, CuratorWatcherImpl genericChildWatchListener) {
        try {
            return client.getChildren().usingWatcher(genericChildWatchListener).forPath(path);
        } catch (KeeperException.NoNodeException e) {
            return null;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public CuratorZookeeperClient.NodeCacheListenerImpl createGenericDataListener(String path, DataListener k) {
        return new NodeCacheListenerImpl(client,k,path);
    }

    @Override
    public void addGenericDataListener(String path, CuratorZookeeperClient.NodeCacheListenerImpl nodeCacheListener) {
        NodeCache nodeCache = new NodeCache(client,path);
        if(nodeCacheMap.putIfAbsent(path,nodeCache) != null) {
            return ;
        }
        nodeCache.getListenable().addListener(nodeCacheListener);
        try {
            nodeCache.start();
        } catch (Exception e) {
            throw new IllegalStateException("Add nodeCache listener for path:" + path, e);
        }
    }

    @Override
    public void removeGenericDataListener(String path, CuratorZookeeperClient.NodeCacheListenerImpl nodeCacheListener) {
        NodeCache nodeCache = nodeCacheMap.get(path);
        if (nodeCache != null) {
            nodeCache.getListenable().removeListener(nodeCacheListener);
        }
        nodeCacheListener.dataListener = null;
        nodeCacheMap.remove(path);
    }

    @Override
    public void removeGenericChilidListener(String path, CuratorZookeeperClient.PathChildCacheListenerImpl pathChildCacheListener) {
        PathChildrenCache pathChildrenCache = childNodeCacheMap.get(path);
        if(pathChildCacheListener != null) {
            pathChildrenCache.getListenable().removeListener(pathChildCacheListener);
        }
        childNodeCacheMap.remove(path);
    }

    @Override
    public List<String> getChildren(String path) {
        try {
            return client.getChildren().forPath(path);
        } catch (KeeperException.NoNodeException e) {
            return null;
        } catch (Exception e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    @Override
    public boolean checkExists(String path) {
        try {
            return client.checkExists().forPath(path) != null;
        } catch (Exception e) {
        }
        return false;
    }


    public class NodeCacheListenerImpl implements NodeCacheListener {

        private CuratorFramework client;

        //会被其他线程进行修改
        private volatile DataListener dataListener;

        private String path;

        public NodeCacheListenerImpl() {}

        public NodeCacheListenerImpl(CuratorFramework client, DataListener dataListener, String path) {
            this.client = client;
            this.dataListener = dataListener;
            this.path = path;
        }

        @Override
        public void nodeChanged() {
            logger.info("NodeCacheListenerImpl node chanage path " + path);
            ChildData childData = nodeCacheMap.get(path).getCurrentData();
            String content = null;
            EventType eventType;
            if (childData == null) {
                eventType = EventType.NodeDeleted;
            } else {
                content = new String(childData.getData(), CHARSET);
                eventType = EventType.NodeDataChanged;
            }
            dataListener.dataChanged(path, content, eventType);
        }
    }

    public class PathChildCacheListenerImpl implements PathChildrenCacheListener {

        private volatile ChildListener childListener;

        public PathChildCacheListenerImpl(ChildListener childListener) {
            this.childListener = childListener;
        }

        @Override
        public void childEvent(CuratorFramework client, PathChildrenCacheEvent event) throws Exception {
            logger.info("-----PathChildCache---  "+ LocalDateTime.now()+"---"+event.getType());
            if(event.getType().equals(PathChildrenCacheEvent.Type.INITIALIZED)){
                logger.info("子节点初始化成功...");
            }else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_ADDED)){
                String path = event.getData().getPath();
                logger.info("添加子节点:" + event.getData().getPath());
                logger.info("子节点数据:" + (event.getData().getData() != null ? new String(event.getData().getData()) : "null"));
            }else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_REMOVED)){
                logger.info("删除子节点:" + event.getData().getPath());
            }else if(event.getType().equals(PathChildrenCacheEvent.Type.CHILD_UPDATED)){
                logger.info("修改子节点路径:" + event.getData().getPath());
                logger.info("修改子节点数据:" + new String(event.getData().getData()));
            }
            if(event.getData() != null) {
                childListener.childChanged(event.getData().getPath(),new String(event.getData().getData()), event.getType());
            }
        }
    }


    public class CuratorWatcherImpl implements CuratorWatcher  {

        private CuratorFramework client;

        private volatile ChildWatchListener childListener;

        private String path;

        public CuratorWatcherImpl(CuratorFramework client, ChildWatchListener childListener, String path) {
            this.client = client;
            this.childListener = childListener;
            this.path = path;
        }

        @Override
        public void process(WatchedEvent event) throws Exception {
            logger.info("-----CuratorWatcherImpl---  "+ LocalDateTime.now()+"---"+event.getType().name());
            if (event.getType() == Watcher.Event.EventType.None) {
                return;
            }
            if (childListener != null) {
                childListener.childChanage(path, client.getChildren().usingWatcher(this).forPath(path));
            }
        }
    }


    /**
     * zookeeper connectstatelistener
     */
    private class CuratorConnectionStateListener implements org.apache.curator.framework.state.ConnectionStateListener {

        private final long UNKNOWN_SESSION_ID = -1;

        private long lastSessionId;

        @Override
        public void stateChanged(CuratorFramework client, ConnectionState state) {
            long sessionId = UNKNOWN_SESSION_ID;
            try {
                sessionId = client.getZookeeperClient().getZooKeeper().getSessionId();
            } catch (Exception e) {
                logger.warn("Curator client state changed, but failed to get the related zk session instance.");
            }
            if(ConnectionState.LOST == state) {
                logger.warn("Curator zookeeper session " + Long.toHexString(lastSessionId) + " expired.");
            } else if (ConnectionState.SUSPENDED == state) {
                logger.warn("Curator zookeeper connection of session " + Long.toHexString(sessionId) + " timed out. " +
                        "connection timeout value is " + DEFAULT_CONNECTION_TIMEOUT_MS + ", session expire timeout value is " + DEFAULT_SESSION_TIMEOUT_MS);
            } else if (ConnectionState.CONNECTED == state) {
                lastSessionId = sessionId;
                logger.info("Curator zookeeper client instance initiated successfully, session id is " + Long.toHexString(sessionId));
            } else if (ConnectionState.RECONNECTED == state) {
                if (lastSessionId == sessionId && sessionId != UNKNOWN_SESSION_ID) {
                    logger.warn("Curator zookeeper connection recovered from connection lose, " +
                            "reuse the old session " + Long.toHexString(sessionId));
                } else {
                    logger.warn("New session created after old session lost, " +
                            "old session " + Long.toHexString(lastSessionId) + ", new session " + Long.toHexString(sessionId));
                    lastSessionId = sessionId;
                }
            }
        }
    }

}
