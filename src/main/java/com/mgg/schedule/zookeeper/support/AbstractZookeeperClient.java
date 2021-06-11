package com.mgg.schedule.zookeeper.support;

import com.mgg.schedule.util.ConcurrentHashSet;
import com.mgg.schedule.zookeeper.ChildListener;
import com.mgg.schedule.zookeeper.ChildWatchListener;
import com.mgg.schedule.zookeeper.DataListener;
import com.mgg.schedule.zookeeper.ZookeeperClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 抽象zookeeper接口
 * @param <GenericDataListener>
 * @param <GenericChildListener>
 */
public abstract class AbstractZookeeperClient<GenericDataListener,GenericChildListener,GenericChildWatchListener> implements ZookeeperClient {

    protected static final Logger logger = LoggerFactory.getLogger(AbstractZookeeperClient.class);
    protected int DEFAULT_CONNECTION_TIMEOUT_MS = 5 * 1000;
    protected int DEFAULT_SESSION_TIMEOUT_MS = 60 * 1000;

    private final String address;

    private final ConcurrentMap<String,ConcurrentMap<ChildListener,GenericChildListener>> childListeners = new ConcurrentHashMap<>();

    private final ConcurrentMap<String,ConcurrentMap<DataListener,GenericDataListener>> dataListeners = new ConcurrentHashMap<>();

    private final ConcurrentMap<String,ConcurrentMap<ChildWatchListener,GenericChildWatchListener>> childWatchListeners = new ConcurrentHashMap<>();

    private final Set<String> persistentExistNodePath = new ConcurrentHashSet<>();

    protected AbstractZookeeperClient(String address) {
        this.address = address;
    }

    public String getAddress() {
        return address;
    }

    @Override
    public String getContent(String path) {
        if (!checkExists(path)) {
            return null;
        }
        return doGetContent(path);
    }

    protected abstract String doGetContent(String path);

    @Override
    public void create(String path, boolean ephemeral) {
        if (!ephemeral) {
            if(persistentExistNodePath.contains(path)) {
                return ;
            }
            if (checkExists(path)) {
                persistentExistNodePath.add(path);
                return ;
            }
        }
        int i = path.lastIndexOf('/');
        if (i > 0) {
            create(path.substring(0,i),false);
        }
        
        if (ephemeral) {
            createEphemeral(path);
        } else {
            createPersistent(path);
            persistentExistNodePath.add(path);
        }
    }

    @Override
    public void create(String path, String content, boolean ephemeral) {
        if (checkExists(path)) {
            delete(path);
        }
        int i = path.lastIndexOf('/');
        if (i > 0) {
            create(path.substring(0, i),ephemeral);
        }
        if (ephemeral) {
            createEphemeral(path, content);
        } else {
            createPersistent(path, content);
        }
    }

    protected abstract void createPersistent(String path);

    protected abstract void createEphemeral(String path);

    protected abstract void createPersistent(String path, String content);

    protected abstract void createEphemeral(String path, String content);

    @Override
    public void delete(String path) {
        persistentExistNodePath.remove(path);
        deletePath(path);
    }

    protected abstract void deletePath(String path);

    @Override
    public void addChildListener(String path, final ChildListener listener) {
        ConcurrentMap<ChildListener,GenericChildListener> listeners = childListeners.computeIfAbsent(path,k -> new ConcurrentHashMap<>());
        GenericChildListener genericChildListener = listeners.computeIfAbsent(listener,k -> createGenericChildListener(path,k));
        addGenericChildListener(path,genericChildListener);
    }

    protected abstract GenericChildListener createGenericChildListener(String path, ChildListener k);

    protected abstract void addGenericChildListener(String path, GenericChildListener genericChildListener);


    protected abstract GenericChildWatchListener createGenericChildWatchListener(String path, ChildWatchListener k);

    protected abstract List<String> addGenericChildWatchListener(String path, GenericChildWatchListener genericChildWatchListener);

    @Override
    public void addDataListener(String path, DataListener listener) {
        ConcurrentMap<DataListener,GenericDataListener> listeners = dataListeners.computeIfAbsent(path,k -> new ConcurrentHashMap<>());
        GenericDataListener genericDataListener = listeners.computeIfAbsent(listener,k -> createGenericDataListener(path,k));
        addGenericDataListener(path,genericDataListener);
    }

    @Override
    public void addChildWatchListener(String path, ChildWatchListener listener) {
        ConcurrentMap<ChildWatchListener,GenericChildWatchListener> listeners = childWatchListeners.computeIfAbsent(path,k -> new ConcurrentHashMap<>());
        GenericChildWatchListener genericDataListener = listeners.computeIfAbsent(listener,k -> createGenericChildWatchListener(path,k));
        addGenericChildWatchListener(path,genericDataListener);
    }

    protected abstract GenericDataListener createGenericDataListener(String path, DataListener k);

    protected abstract void addGenericDataListener(String path, GenericDataListener genericDataListener);

    @Override
    public void removeDataListener(String path, DataListener listener) {
        ConcurrentMap<DataListener,GenericDataListener> dataListenerConcurrentMap = dataListeners.get(path);
        if(dataListenerConcurrentMap != null) {
            GenericDataListener genericDataListener = dataListenerConcurrentMap.remove(listener);
            if(genericDataListener != null) {
                removeGenericDataListener(path,genericDataListener);
            }
        }
    }

    protected abstract void removeGenericDataListener(String path, GenericDataListener genericDataListener);

    @Override
    public void removeChildListener(String path, ChildListener listener) {
        ConcurrentMap<ChildListener,GenericChildListener> childListenerConcurrentMap = childListeners.get(path);
        if(childListenerConcurrentMap != null) {
            GenericChildListener genericChildListener = childListenerConcurrentMap.remove(listener);
            if(genericChildListener != null) {
                removeGenericChilidListener(path,genericChildListener);
            }
        }
    }

    protected abstract void removeGenericChilidListener(String path, GenericChildListener genericChildListener);
}
