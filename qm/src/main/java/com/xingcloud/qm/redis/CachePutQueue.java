package com.xingcloud.qm.redis;

import java.util.concurrent.LinkedBlockingQueue;

import com.xingcloud.cache.MappedXCache;

public class CachePutQueue {

    private static CachePutQueue instance;

    private CachePutQueue() {

    }

    public static synchronized CachePutQueue getInstance() {
        if (instance == null) {
            instance = new CachePutQueue();
        }
        return instance;
    }

    private final LinkedBlockingQueue<MappedXCache> cacheQueue = new LinkedBlockingQueue<MappedXCache>();

    public void putQueue( MappedXCache cache ) throws InterruptedException {
        cacheQueue.put(cache);
    }

    public MappedXCache take() throws InterruptedException {
        return cacheQueue.take();
    }

}
