package com.xingcloud.qm.config;

import com.xingcloud.xa.conf.Config;
import org.apache.commons.configuration.Configuration;

public class QMConfig {
  private static QMConfig instance;

  public static final String DEFAULT_CONF = "/qm.properties";
  public static final String OVERRIDE_CONF = "/conf/qm.properties";
  public static final String PROP_CONF = "qm.conf";
  public static final String DRILL_EXEC_TIMEOUT = "drill.exec.timeout";
  public static final String TEMPDIR = "qm.tempdir";
    //同时最多允许多少个plan执行
  public static final String MAX_PLAN_EXECUTING = "qm.maxplan.total";

  //每个project，同时最多允许多少个plan执行
  public static final String MAX_PLAN_PER_PROJECT = "qm.maxplan.perproject";

  //最多允许多少个plan一起合并
  public static final String MAX_BATCHMERGE = "qm.maxplan.permerge";

  //最多允许的合并后的plan的cost。目前，单个原始查询的cost为1。
  public static final String MAX_BATCHCOST = "qm.maxcost";

  // 是否在查询结束后存放缓存
  public static final String USING_CACHE = "qm.using_cache";

  //是否输出合并前logical plan和合并后的logical plan
  public static final String WRITE_PLAN = "qm.write_plans";


  private Configuration conf;

  public QMConfig() {
    conf = Config.createConfig(DEFAULT_CONF, OVERRIDE_CONF, PROP_CONF, Config.ConfigFormat.properties);
  }

  public static QMConfig getInstance() {
    if(instance == null){
      instance = new QMConfig();
    }
    return instance;
  }
  
  public static Configuration conf(){
    return getInstance().conf;
  }
}
