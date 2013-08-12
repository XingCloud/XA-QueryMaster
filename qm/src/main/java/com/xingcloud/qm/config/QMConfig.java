package com.xingcloud.qm.config;

import com.xingcloud.xa.conf.Config;
import org.apache.commons.configuration.Configuration;

public class QMConfig {
  private static QMConfig instance;

  public static final String DEFAULT_CONF = "qm.properties";
  public static final String OVERRIDE_CONF = "/conf/qm.properties";
  public static final String PROP_CONF = "qm.conf";
  public static final String DRILL_EXEC_TIMEOUT = "drill.exec.timeout";
  
  
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
