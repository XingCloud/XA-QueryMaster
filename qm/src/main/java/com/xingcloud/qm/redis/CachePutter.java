package com.xingcloud.qm.redis;

import static com.xingcloud.qm.utils.QueryMasterVariables.PUT_CACHE;

import com.xingcloud.cache.MappedXCache;
import com.xingcloud.cache.XCacheOperator2;
import com.xingcloud.cache.exception.XCacheException;
import com.xingcloud.cache.redis.RedisXCacheOperator2;
import org.apache.log4j.Logger;

import java.util.Map.Entry;

public class CachePutter implements Runnable {
  private static final Logger LOGGER = Logger.getLogger(CachePutter.class);

  @Override
  public void run() {
    XCacheOperator2 operator = RedisXCacheOperator2.getInstance();
    CachePutQueue queue = CachePutQueue.getInstance();
    MappedXCache cache;
    try {
      while (true) {
        cache = queue.take();
        if (cache == null) {
          continue;
        }
        if (PUT_CACHE) {
          try {
            long t1 = System.currentTimeMillis();
            operator.putMappedCache(cache);
            long t2 = System.currentTimeMillis();
            LOGGER.info("[CACHE-PUTTER] - Put cache ok - " + cache.getKey() + " in " + (t2 - t1) + " milliseconds.");
          } catch (XCacheException e) {
            LOGGER.error("[CACHE-PUTTER] - Failed to put cache - " + cache, e);
            continue;
          }
        } else {
          LOGGER.info("[CACHE-PUTTER] - Put cache ok(test) - " + cache.getKey());
          for (Entry<String, String> entry : cache.getValue().entrySet()) {
            LOGGER.info("[CACHE-PUTTER] - " + entry.getKey() + "=" + entry.getValue());
          }
        }
      }
    } catch (InterruptedException e) {
      LOGGER.info("[CACHE-PUTTER] - Cache putter(" + Thread.currentThread().getName() + ") will be shutdown.");
      Thread.currentThread().interrupt();
    }
  }

}
