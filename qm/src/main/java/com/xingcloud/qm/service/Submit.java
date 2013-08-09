package com.xingcloud.qm.service;

import com.xingcloud.qm.exceptions.XRemoteQueryException;

import java.io.Serializable;

public interface Submit {
  public static enum SubmitQueryType implements Serializable {
    SQL, PLAN
  }

  public boolean submit(String cacheKey, String content, SubmitQueryType type) throws XRemoteQueryException;
}
