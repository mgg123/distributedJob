package com.mgg.schedule.util;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * copy from {@link java.util.HashSet}
 * @param <E>
 */
public class ConcurrentHashSet<E> extends AbstractSet<E> implements Set<E>, Serializable {

    static final long serialVersionUID = -1;

    private transient final ConcurrentHashMap<E,Object> map;

    private static final Object PRESENT = new Object();

    public ConcurrentHashSet() {
        this.map = new ConcurrentHashMap<E,Object>();
    }

    public ConcurrentHashSet(int initialCapacity) {
        map = new ConcurrentHashMap<E, Object>(initialCapacity);
    }

    @Override
    public Iterator<E> iterator() {
        return map.keySet().iterator();
    }

    @Override
    public int size() {
        return map.size();
    }

    @Override
    public boolean isEmpty() {
        return map.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return map.contains(o);
    }

    @Override
    public boolean add(E e) {
        return map.put(e,PRESENT) == null;
    }

    @Override
    public boolean remove(Object o) {
        return map.remove(o) == PRESENT;
    }

    @Override
    public void clear() {
        map.clear();
    }
}
