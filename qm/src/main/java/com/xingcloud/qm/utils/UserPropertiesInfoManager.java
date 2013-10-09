package com.xingcloud.qm.utils;

import com.google.common.base.Strings;
import com.xingcloud.memcache.MemCacheException;
import com.xingcloud.memcache.XMemCacheManager;
import com.xingcloud.mysql.MySql_16seqid;
import com.xingcloud.mysql.UserProp;
import com.xingcloud.qm.cache.MemoryCachedObjects;
import static com.xingcloud.qm.cache.MemoryCachedObjects.*;

import com.xingcloud.qm.exceptions.UserPropertyException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

import java.sql.SQLException;
import java.util.List;


public class UserPropertiesInfoManager {
  private static final Logger LOGGER = Logger.getLogger(UserPropertiesInfoManager.class);
  private static final UserPropertiesInfoManager instance = new UserPropertiesInfoManager();

  private UserPropertiesInfoManager() {
  }

  public static UserPropertiesInfoManager getInstance() {
    return instance;
  }

  private final XMemCacheManager xMemCacheManager = MemoryCachedObjects.getInstance().getCacheManager();

  public String getKey(String projectId, String propertyName) {
    return projectId + '.' + propertyName;
  }

  public synchronized List<UserProp> putAllUPToCache(String projectId) throws SQLException, InterruptedException,
    MemCacheException {
    List<UserProp> props = MySql_16seqid.getInstance().getUserProps(projectId);
    for (UserProp up : props) {
      xMemCacheManager.putCacheElement(MCO_USER_PROPERTIES, getKey(projectId, up.getPropName()), up);
      LOGGER.info("[UP-CACHE] - User property of " + projectId + " has been saved to cache(" + up.getPropName() + ")");
    }
    return props;
  }

  private UserProp loadAllReturnOne(String projectId, String propertyName) throws SQLException, MemCacheException,
    InterruptedException {
    List<UserProp> props = putAllUPToCache(projectId);
    if (CollectionUtils.isEmpty(props)) {
      return null;
    }
    for (UserProp upup : props) {
      if (propertyName.equals(upup.getPropName())) {
        return upup;
      }
    }
    return null;
  }

  public synchronized UserProp getUserProp(String projectId, String propertyName) throws UserPropertyException {
    if (Strings.isNullOrEmpty(projectId)) {
      throw new UserPropertyException("Cannot get user properties - project id is empty.");
    }
    if (Strings.isNullOrEmpty(propertyName)) {
      throw new UserPropertyException("Cannot get user property - property name is empty.");
    }
    String k = getKey(projectId, propertyName);
    UserProp up;
    try {
      up = xMemCacheManager.getCacheElement(MCO_USER_PROPERTIES, k, UserProp.class);
    } catch (MemCacheException e) {
      throw new UserPropertyException(e);
    }
    if (up != null) {
      return up;
    }

    try {
      up = loadAllReturnOne(projectId, propertyName);
    } catch (Exception e) {
      throw new UserPropertyException(e);
    }
    if (up == null) {
      throw new UserPropertyException("No such property(" + propertyName + ") for project " + projectId);
    }
    return up;
  }

}
