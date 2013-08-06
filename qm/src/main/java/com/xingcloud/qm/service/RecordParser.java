package com.xingcloud.qm.service;

import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.ValueVector;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordParser {

  public static final String COL_COUNT = "count";
  public static final String COL_SUM = "sum";
  public static final String COL_USER_NUM = "user_num";
  public static final String COL_DIMENSION = "dimension";
  public static final String COL_QUERYID = "queryid";
  /**
   * 按照XCache约定格式，解析返回的RecordBatch
   * @param records 
   * @param allocator
   * @return Map: queryID -> value. value的类型是：Map<String, Number[]> 包含count, sum, user_num
   */
  public static Map<String, Map<String, Number[]>> materializeRecords(List<QueryResultBatch> records, BufferAllocator allocator) {
    Map<String, Map<String, Number[]>> out = new HashMap<>();
    // Look at records
    RecordBatchLoader batchLoader = new RecordBatchLoader(allocator);
    int recordCount = 0;
    String dimensionKey = null;
    long count, sum, user_num = 0;
    String currentQueryID = null;
    Map<String, Number[]> currentValue = new HashMap<>();
    for (QueryResultBatch batch : records) {
      if (!batch.hasData()) continue;
      boolean schemaChanged = false;
      try {
        schemaChanged = batchLoader.load(batch.getHeader().getDef(), batch.getData());
      } catch (SchemaChangeException e) {
        e.printStackTrace();  //e:
      }

      for (int i = 0; i < batchLoader.getRecordCount(); i++) {
        recordCount++;
        dimensionKey = "";
        count = 0; sum=0; user_num=0;
        for (ValueVector vv : batchLoader) {
          String colName = vv.getField().getName();
          if(COL_QUERYID.equals(colName)){
            String nextQueryID = (String) vv.getAccessor().getObject(i);
            if(!nextQueryID.equals(currentQueryID)){
              //new queryID
              if(currentQueryID != null){
                //output previous queryID
                out.put(currentQueryID, currentValue);
                currentQueryID = nextQueryID;
                currentValue = new HashMap<>();
              }
            }
          }else if(COL_DIMENSION.equals(colName)){
            dimensionKey = (String) vv.getAccessor().getObject(i);
          }else if(COL_COUNT.equals(colName)){
            count = (long) vv.getAccessor().getObject(i);
          }else if(COL_SUM.equals(colName)){
            sum = (long) vv.getAccessor().getObject(i);
          }else if(COL_USER_NUM.equals(colName)){
            user_num = (long) vv.getAccessor().getObject(i); 
          }
          currentValue.put(dimensionKey, new Number[]{count, sum, user_num});
        }
      }//batchLoader.getRecordCount()
      
    }//for records
    if(currentQueryID != null){
      //output previous queryID
      out.put(currentQueryID, currentValue);
    }
    return out;
  }
}
