package com.xingcloud.qm.cache;

import com.xingcloud.memcache.XMemCacheManager;
import com.xingcloud.xa.conf.Config;
import org.apache.commons.configuration.Configuration;
import org.apache.log4j.Logger;

/**
 * User: Z J Wu Date: 13-8-26 Time: 上午11:21 Package: com.xingcloud.webinterface.cache
 */
public class MemoryCachedObjects {
  private static final Logger LOGGER = Logger.getLogger(MemoryCachedObjects.class);

  private MemoryCachedObjects() {
    init();
  }

  private static final MemoryCachedObjects instance = new MemoryCachedObjects();

  public static MemoryCachedObjects getInstance() {
    return instance;
  }

  public static String MCO_META_TABLE;
  public static String MCO_USER_PROPERTIES;

  private XMemCacheManager cacheManager = new XMemCacheManager();

  public XMemCacheManager getCacheManager() {
    return cacheManager;
  }

  private void init() {
    Configuration config = Config.createConfig("/ehcache.webinterface.properties", Config.ConfigFormat.properties);

    MCO_META_TABLE = config.getString("meta-table.cache.name");
    int cacheNum = config.getInt("meta-table.cache.num", 10000);
    boolean cacheOverflowToDisk = config.getBoolean("meta-table.cache.overflowToDisk", false);
    boolean cacheEternal = config.getBoolean("meta-table.cache.eternal", false);
    int cacheTimeToIdleSeconds = config.getInt("meta-table.cache.timeToIdleSeconds", 300);
    int cacheTimeToLiveSeconds = config.getInt("meta-table.cache.timeToLiveSeconds", 600);
    cacheManager.addCache(MCO_META_TABLE, cacheNum, cacheOverflowToDisk, cacheEternal, cacheTimeToIdleSeconds,
                          cacheTimeToLiveSeconds);
    LOGGER.info("[MEM-CACHE] - Cache(" + MCO_META_TABLE + ") is added to cache manager and standby.");

    MCO_USER_PROPERTIES = config.getString("user-properties.cache.name");
    cacheNum = config.getInt("user-properties.cache.num", 10000);
    cacheOverflowToDisk = config.getBoolean("user-properties.cache.overflowToDisk", false);
    cacheEternal = config.getBoolean("user-properties.cache.eternal", false);
    cacheTimeToIdleSeconds = config.getInt("user-properties.cache.timeToIdleSeconds", 300);
    cacheTimeToLiveSeconds = config.getInt("user-properties.cache.timeToLiveSeconds", 600);
    cacheManager.addCache(MCO_USER_PROPERTIES, cacheNum, cacheOverflowToDisk, cacheEternal, cacheTimeToIdleSeconds,
                          cacheTimeToLiveSeconds);
    LOGGER.info("[MEM-CACHE] - Cache(" + MCO_USER_PROPERTIES + ") is added to cache manager and standby.");
  }
}
