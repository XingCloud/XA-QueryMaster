package com.xingcloud.qm.service;

import com.xingcloud.qm.exceptions.XRemoteQueryException;
import com.xingcloud.qm.result.ResultRow;
import com.xingcloud.qm.result.ResultTable;
import org.apache.drill.exec.exception.SchemaChangeException;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.proto.UserBitShared.DrillPBError;
import org.apache.drill.exec.proto.UserProtos.QueryResult.QueryState;
import org.apache.drill.exec.record.RecordBatchLoader;
import org.apache.drill.exec.rpc.user.QueryResultBatch;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.ValueVector;
import org.apache.drill.exec.vector.VarCharVector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RecordParser {

  final static Logger logger = LoggerFactory.getLogger(RecordParser.class);

  public static final String COL_COUNT = "count";
  public static final String COL_SUM = "sum";
  public static final String COL_USER_NUM = "user_num";
  public static final String COL_DIMENSION = "dimension";
  public static final String COL_QUERYID = "query_id";

  /**
   * 按照XCache约定格式，解析返回的RecordBatch
   *
   * @param records
   * @param allocator
   * @return Map: queryID -> value. value的类型是：Map<String, Number[]> 包含count, sum, user_num, 以及一个采样率。
   */
  public static Map<String, ResultTable> materializeRecords(List<QueryResultBatch> records, BufferAllocator allocator) throws Exception {
    Map<String, ResultTable> out = new HashMap<>();
    // Look at records
    RecordBatchLoader batchLoader = new RecordBatchLoader(allocator);
    int recordCount = 0;
    String dimensionKey = null;
    long count, sum, user_num = 0;
    String currentQueryID = null;
    ResultTable currentValue = new ResultTable();
    for (QueryResultBatch batch : records) {
      QueryState queryState = batch.getHeader().getQueryState();
      if (queryState == QueryState.FAILED) {
        String errMsg = "";
        for (DrillPBError error : batch.getHeader().getErrorList()) {
          errMsg += error.getMessage() + " ";
        }
        logger.error("Query " + batch.getHeader().getQueryId() + "failed :" + errMsg);
        throw new XRemoteQueryException(errMsg);
      }
      if (!batch.hasData()) continue;
      boolean schemaChanged = false;
      try {
        schemaChanged = batchLoader.load(batch.getHeader().getDef(), batch.getData());
      } catch (SchemaChangeException e) {
        e.printStackTrace();  //e:
      }

      BigIntVector countVector = null, userNumVector = null, sumVector = null;
      VarCharVector queryIdVector = null;
      ValueVector demensionVector = null;
      for (ValueVector vv : batchLoader) {
        String columnName = vv.getField().getName();
        if (COL_COUNT.equals(columnName)) {
          countVector = (BigIntVector) vv;
        } else if (COL_USER_NUM.equals(columnName)) {
          userNumVector = (BigIntVector) vv;
        } else if (COL_SUM.equals(columnName)) {
          sumVector = (BigIntVector) vv;
        } else if (COL_QUERYID.equals(columnName)) {
          queryIdVector = (VarCharVector) vv;
        } else if (COL_DIMENSION.equals(columnName)) {
          demensionVector = vv;
        }
      }

      for(int i = 0 ; i < batchLoader.getRecordCount() ;i ++){
        recordCount ++ ;
        String demesionKey = "" ;
        if(demensionVector != null){
          Object demension = demensionVector.getAccessor().getObject(i) ;
          if(demension != null){
            if(demension instanceof  byte[]){
              demesionKey = new String((byte[]) demension);
            }else{
              demesionKey = String.valueOf(demension);
            }
          } else{
            demesionKey = "XA-NA" ;
          }
        }
        count = countVector.getAccessor().get(i) ;
        sum = sumVector.getAccessor().get(i);
        user_num = userNumVector.getAccessor().get(i);
        String nextQueryID = new String((byte[]) queryIdVector.getAccessor().get(i)) ;
        if(!nextQueryID.equals(currentQueryID)){
           if(currentQueryID != null){
             out.put(currentQueryID,currentValue);
             currentValue = new ResultTable() ;
           }
           currentQueryID = nextQueryID ;
        }
        currentValue.put(demesionKey,new ResultRow(count,sum,user_num));
      }

      for(ValueVector v : batchLoader){
        v.close();
      }

    }//for records
    if (currentQueryID != null) {
      //output previous queryID
      out.put(currentQueryID, currentValue);
    }
    return out;
  }
}
