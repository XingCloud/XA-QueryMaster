package com.xingcloud.qm.redis;

import static com.xingcloud.qm.utils.QueryMasterConstant.*;
import org.apache.log4j.Logger;

import com.xingcloud.cache.MappedXCache;
import com.xingcloud.cache.XCacheOperator2;
import com.xingcloud.cache.exception.XCacheException;
import com.xingcloud.cache.redis.RedisXCacheOperator2;

public class CachePutter implements Runnable {
    private static final Logger LOGGER = Logger.getLogger(CachePutter.class);

    @Override
    public void run() {
        XCacheOperator2 operator = RedisXCacheOperator2.getInstance();
        CachePutQueue queue = CachePutQueue.getInstance();
        MappedXCache cache = null;
        try {
            while (true) {
                cache = queue.take();
                if (cache == null) {
                    continue;
                }
                if (PUT_CACHE) {
                    try {
                        operator.putMappedCache(cache);
                        LOGGER.info("[CACHE-PUTTER] - Put cache ok - "
                                + cache.getKey());
                    } catch (XCacheException e) {
                        LOGGER.error("[CACHE-PUTTER] - Failed to put cache - "
                                + cache, e);
                        continue;
                    }
                } else {
                    LOGGER.info("[CACHE-PUTTER] - Put cache ok(degub) - "
                            + cache.getKey());
                }
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            Thread.currentThread().interrupt();
        }
    }

}
