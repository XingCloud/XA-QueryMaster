package com.xingcloud.qm.utils;

import java.util.Collection;

/**
 * User: Z J Wu Date: 13-8-6 Time: 下午6:51 Package: com.xingcloud.webinterface.utils
 */
public class SerializationUtils {
  public static void addAll(Collection collection, byte[] bytes) {
    for (int i = 0, size = bytes.length; i < size; i++) {
      collection.add(bytes[i]);
    }
  }
}
