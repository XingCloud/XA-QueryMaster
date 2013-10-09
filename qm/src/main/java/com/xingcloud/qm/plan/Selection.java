package com.xingcloud.qm.plan;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.ProxyMetaClientFactory;
import com.xingcloud.meta.TableInfo;
import org.apache.commons.lang.ArrayUtils;
import org.apache.drill.common.JSONOptions;
import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.common.logical.data.NamedExpression;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.thrift.TException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;
import static com.xingcloud.meta.KeyPart.Type;
import static com.xingcloud.qm.utils.SerializationUtils.*;
import static org.apache.drill.common.util.Selections.*;

/**
 * User: Z J Wu Date: 13-7-24 Time: 上午10:49 Package: org.apache.drill.common.util
 */

@JsonInclude(NON_NULL)
public class Selection {

  public static final IMetaStoreClient CLIENT = ProxyMetaClientFactory.getInstance().newProxiedPooledClient();

  private String dbName;
  private String tableName;

  private RowkeyRange rowkey;

  private ScanFilterDescriptor[] filters;

  private NamedExpression[] projections;

  public Selection(String dbName, String tableName, RowkeyRange rowkey, NamedExpression[] projections) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.rowkey = rowkey;
    this.projections = projections;
  }

  public Selection(String dbName, String tableName, RowkeyRange rowkey, NamedExpression[] projections,
                   ScanFilterDescriptor... filters) {
    this.dbName = dbName;
    this.tableName = tableName;
    this.rowkey = rowkey;
    this.projections = projections;
    this.filters = filters;
  }

  public Selection(String tableName, RowkeyRange rowkey, NamedExpression[] projections) {
    this.tableName = tableName;
    this.rowkey = rowkey;
    this.projections = projections;
  }

  public Selection(String tableName, RowkeyRange rowkey, NamedExpression[] projections,
                   ScanFilterDescriptor... filters) {
    this.tableName = tableName;
    this.rowkey = rowkey;
    this.projections = projections;
    this.filters = filters;
  }

  private static void extractColumns(List<KeyPart> pkKeyParts, List<KeyPart> allKeyParts) {
    Type type;
    for (KeyPart kp : pkKeyParts) {
      type = kp.getType();
      if (Type.constant.equals(type) || Type.field.equals(type)) {
        allKeyParts.add(kp);
      }
      // Optional Group
      else {
        List<KeyPart> optional = kp.getOptionalGroup();
        extractColumns(optional, allKeyParts);
      }
    }
  }

  public static RowkeyRange toRowkeyRange(Table table, Map<String, KeyPartParameter> parameterMap) throws TException {

    int[] parameterLength = keyPartParameterLength(parameterMap);
    List<KeyPart> pkKeyParts = TableInfo.getPrimaryKey(table), allKeyParts = new ArrayList<KeyPart>();

    extractColumns(pkKeyParts, allKeyParts);

    List<Byte> startKeyList = new ArrayList<Byte>(20), endKeyList = new ArrayList<Byte>(20);
    Type keyPartType;
    // There has no optional keys.
    String fieldName;
    byte[] keyPartValue1, keyPartValue2;
    KeyPartParameter keyPartParameter;

    int resolvedStartCount = 0, resolvedEndCount = 0;
    int startParamSize = parameterLength[0], endParamSize = parameterLength[1];

    for (KeyPart kp : allKeyParts) {
      keyPartType = kp.getType();
      if (Type.field.equals(keyPartType)) {
        fieldName = kp.getField().getName();
        keyPartParameter = parameterMap.get(fieldName);
        if (keyPartParameter == null) {
          continue;
        }
        if (keyPartParameter.isSingle()) {
          keyPartValue1 = keyPartParameter.getParameterValue1();
          if (keyPartValue1 == null) {
            break;
          } else {
            if (resolvedStartCount < startParamSize) {
              addAll(startKeyList, keyPartValue1);
              ++resolvedStartCount;
            }
            if (resolvedEndCount < endParamSize) {
              addAll(endKeyList, keyPartValue1);
              ++resolvedEndCount;
            }
          }
        } else {
          keyPartValue1 = keyPartParameter.getParameterValue1();
          keyPartValue2 = keyPartParameter.getParameterValue2();
          if (resolvedStartCount < startParamSize) {
            addAll(startKeyList, keyPartValue1);
            ++resolvedStartCount;
          }
          if (resolvedEndCount < endParamSize) {
            addAll(endKeyList, keyPartValue2);
            ++resolvedEndCount;
          }
        }
      } else {
        if (resolvedStartCount < startParamSize) {
          addAll(startKeyList, kp.getConstant().getBytes());
        }
        if (resolvedEndCount < endParamSize) {
          addAll(endKeyList, kp.getConstant().getBytes());
        }
      }
    }
    int size = startKeyList.size();
    byte[] startKey = new byte[size];
    for (int i = 0; i < size; i++) {
      startKey[i] = startKeyList.get(i);
    }
    size = endKeyList.size();
    byte[] endKey = new byte[size];
    for (int i = 0; i < size; i++) {
      endKey[i] = endKeyList.get(i);
    }
    return RowkeyRange.create(startKey, endKey);
  }

  private static int[] keyPartParameterLength(Map<String, KeyPartParameter> parameterMap) {
    int startParameterCount = 0;
    int endParameterCount = 0;
    KeyPartParameter kpp;
    byte[] v1, v2;
    for (Map.Entry<String, KeyPartParameter> entry : parameterMap.entrySet()) {
      kpp = entry.getValue();
      if (kpp.isSingle()) {
        ++startParameterCount;
        ++endParameterCount;
      } else {
        v1 = kpp.getParameterValue1();
        v2 = kpp.getParameterValue2();
        if (v1 != null) {
          ++startParameterCount;
        }
        if (v2 != null) {
          ++endParameterCount;
        }
      }
    }
    return new int[]{startParameterCount, endParameterCount};
  }

  public String getDbName() {
    return dbName;
  }

  public String getTableName() {
    return tableName;
  }

  public RowkeyRange getRowkey() {
    return rowkey;
  }

  public ScanFilterDescriptor[] getFilters() {
    return filters;
  }

  public NamedExpression[] getProjections() {
    return projections;
  }

  public Map<String, Object> toSelectionMap() throws IOException {
    Map<String, Object> map = new HashMap<String, Object>(4);
    map.put(SELECTION_KEY_WORD_TABLE, this.getTableName());

    Map<String, Object> rowkeyRangeMap = new HashMap<String, Object>(2);
    RowkeyRange range = this.getRowkey();
    rowkeyRangeMap.put(SELECTION_KEY_WORD_ROWKEY_START, range.getStartKey());
    rowkeyRangeMap.put(SELECTION_KEY_WORD_ROWKEY_END, range.getEndKey());
    map.put(SELECTION_KEY_WORD_ROWKEY, rowkeyRangeMap);
    map.put(SELECTION_KEY_WORD_PROJECTIONS, this.getProjections());
    ScanFilterDescriptor[] filters = this.getFilters();
    if (ArrayUtils.isNotEmpty(filters)) {
      List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>(filters.length);
      for (ScanFilterDescriptor filter : filters) {
        mapList.add(filter.toMap());
      }
      map.put(SELECTION_KEY_WORD_FILTERS, mapList);
    }
    return map;
  }

  public JSONOptions toSingleJsonOptions() throws IOException {
    List<Map<String, Object>> mapList = new ArrayList<Map<String, Object>>(1);
    mapList.add(this.toSelectionMap());
    ObjectMapper mapper = DrillConfig.create().getMapper();
    String s = mapper.writeValueAsString(mapList);
    return mapper.readValue(s, JSONOptions.class);
  }

}
