package com.xingcluod.qm.service;

import com.xingcloud.cache.XCache;
import com.xingcloud.cache.XCacheInfo;
import com.xingcloud.cache.XCacheOperator;
import com.xingcloud.cache.exception.XCacheException;
import com.xingcloud.cache.redis.NoSelectRedisXCacheOperator;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * User: Z J Wu Date: 13-8-15 Time: 下午4:05 Package: com.xingcluod.qm.service
 */
public class TestPutCache {

  @Test
  public void testPutCache() throws XCacheException {
    Map<String, Number[]> map = new HashMap<>(1);
    map.put("abc", new Number[]{1, 2, 3, 1d});
    XCacheOperator operator = NoSelectRedisXCacheOperator.getInstance();
    operator.putCache(new XCache("abc", map, System.currentTimeMillis(), XCacheInfo.CACHE_INFO_0));
  }
}
