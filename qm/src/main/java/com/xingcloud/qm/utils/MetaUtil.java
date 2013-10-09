package com.xingcloud.qm.utils;

import com.xingcloud.meta.KeyPart;
import com.xingcloud.meta.TableInfo;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: yb
 * Date: 9/24/13
 * Time: 11:24 AM
 * To change this template use File | Settings | File Templates.
 */
public class MetaUtil {
    private Map<String,List<KeyPart>> tableKps;
    private static MetaUtil instance=null;
    private MetaUtil(){
        tableKps=new HashMap<>();
    }
    public static MetaUtil getInstance(){
        if(instance!=null)
            return instance;
        instance=new MetaUtil();
        return instance;
    }
    public void putTableRkKps(String tableName,List<KeyPart> kps){
        String transTableName;
        if(tableName.contains("deu"))
            transTableName="eventTable";
        else
            transTableName=tableName;
        if(tableKps.containsKey(transTableName))
            return;
        tableKps.put(transTableName,kps);
    }
    public List<KeyPart> getTableRkKps(String tableName) throws Exception {
        String transTableName;
        if(tableName.contains("deu"))
            transTableName="eventTable";
        else
            transTableName=tableName;
        if(!tableKps.containsKey(transTableName))
        {
            List<KeyPart> kps= TableInfo.getRowKey(tableName,null);
            tableKps.put(transTableName,kps);
        }
        return tableKps.get(transTableName);
    }
}
