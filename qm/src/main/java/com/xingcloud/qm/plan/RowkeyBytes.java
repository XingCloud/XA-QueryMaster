package com.xingcloud.qm.plan;

import com.xingcloud.memcache.MemCacheException;
import com.xingcloud.meta.ByteUtils;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;
import com.xingcloud.qm.exceptions.PlanException;
import com.xingcloud.qm.utils.SerializationUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.xingcloud.meta.ByteUtils.toBytes;
import static com.xingcloud.meta.ByteUtils.toStringBinary;

/**
 * User: Z J Wu Date: 13-8-27 Time: 下午3:58 Package: com.xingcloud.webinterface.plan
 */
public class RowkeyBytes {
  private static final Byte[] EMPTY_BYTES = new Byte[0];
  private byte[] rowkeyBytes;

  private RowkeyBytes(byte[] rowkeyBytes) {
    this.rowkeyBytes = rowkeyBytes;
  }

  public byte[] getRowkeyBytes() {
    return rowkeyBytes;
  }

  public void setRowkeyBytes(byte[] rowkeyBytes) {
    this.rowkeyBytes = rowkeyBytes;
  }

  public static List<Byte> keyPart2Bytes(List<KeyPart> kps, Map<String, byte[]> values) {
    List<Byte> byteList = new ArrayList<Byte>(50);

    KeyPart.Type kpType;
    byte[] kpBytes;
    String kpName;
    for (KeyPart kp : kps) {
      kpType = kp.getType();
      if (KeyPart.Type.field.equals(kpType)) {
        kpName = kp.getField().getName();
        kpBytes = values.get(kpName);
        if (kpBytes != null) {
          SerializationUtils.addAll(byteList, kpBytes);
        }
      } else if (KeyPart.Type.constant.equals(kpType)) {

      } else {
        byteList.addAll(keyPart2Bytes(kp.getOptionalGroup(), values));
      }
    }
    return null;
  }

  public static RowkeyBytes create(Table table, Map<String, byte[]> values) throws PlanException,
    UnsupportedEncodingException {
    if (table == null) {
      throw new PlanException("Table cannot be null.");
    }
    if (MapUtils.isEmpty(values)) {
      throw new PlanException("Value map cannot be empty.");
    }
    List<KeyPart> pkKeyParts = TableInfo.getPrimaryKey(table), optionalGroup;
    List<Byte> byteList = new ArrayList<Byte>(50);
    String kpName;
    KeyPart.Type kpType;
    FieldSchema fs;
    byte[] kpBytes;
    for (KeyPart kp : pkKeyParts) {
      kpType = kp.getType();
      if (KeyPart.Type.field.equals(kpType)) {
        kpName = kp.getField().getName();
        kpBytes = values.get(kpName);
        if (kpBytes != null) {
          SerializationUtils.addAll(byteList, kpBytes);
        }
      } else if (KeyPart.Type.constant.equals(kpType)) {
        SerializationUtils.addAll(byteList, toBytes(kp.getConstant()));
      } else {
        optionalGroup = kp.getOptionalGroup();
        for (KeyPart option : optionalGroup) {
          System.out.println(option);
        }
      }
    }
    byte[] bytes = new byte[byteList.size()];
    for (int i = 0; i < byteList.size(); i++) {
      bytes[i] = byteList.get(i);
    }
    return new RowkeyBytes(bytes);
  }

  @Override
  public String toString() {
    return toStringBinary(rowkeyBytes);
  }

  public static void main(String[] args) throws TException, MemCacheException, PlanException,
    UnsupportedEncodingException {
    Map<String, byte[]> values = new HashMap<String, byte[]>();
    values.put("date", ByteUtils.toBytes("20130827"));
    values.put("event0", ByteUtils.toBytes("a"));
    Table table = Plans.getCachedMetaTable("deu_" + Plans.KEY_WORD_GENERIC_EVENT_TABLE_NAME);
    RowkeyBytes rb = RowkeyBytes.create(table, values);
    System.out.println(rb);
  }
}
