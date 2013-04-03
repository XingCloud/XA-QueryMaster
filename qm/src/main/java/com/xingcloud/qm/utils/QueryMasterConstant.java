package com.xingcloud.qm.utils;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.Text;

import com.xingcloud.basic.conf.ConfigReader;
import com.xingcloud.basic.conf.Dom;
import com.xingcloud.qm.thread.ExecutorServiceInfo;

public class QueryMasterConstant {
    public static boolean PUT_CACHE;
    public static final String INNER_HASH_KEY = "XValue";
    public static final String G = "group by";
    public static final int G_LENGTH = G.length();
    public static final Text SIZE_KEY = new Text("size");
    public static final Text META_KEY = new Text("meta");

    public static final int COUNT = 0;
    public static final int SUM = 1;
    public static final int USER_NUM = 2;
    public static final int GROUP = -1;

    public static final String THREAD_PREFIX = "XQueryThread";

    public static Map<String, ExecutorServiceInfo> ESI_MAP;

    static {
        Dom root = ConfigReader.getDom("executor_services.xml");
        Dom esd = root.element("executor-services");
        List<Dom> es = esd.elements("executor-service");
        ESI_MAP = new HashMap<String, ExecutorServiceInfo>(es.size());
        String id = null;
        String name = null;
        int threadCount = 0;
        ExecutorServiceInfo tfi = null;
        for( Dom d: es ) {
            id = d.getAttributeValue("id");
            name = d.elementText("name");
            threadCount = Integer.parseInt(d.elementText("count"));
            tfi = new ExecutorServiceInfo(id, name, threadCount);
            ESI_MAP.put(id, tfi);
        }
        root = ConfigReader.getDom("common.xml");
        String putCacheString = root.element("put-cache").getSelfText();
        PUT_CACHE = Boolean.valueOf(putCacheString);
    }

}
