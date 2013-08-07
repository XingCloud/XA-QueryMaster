package com.xingcloud.qm.service;

import org.apache.drill.common.config.DrillConfig;
import org.apache.drill.exec.client.DrillClient;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.memory.DirectBufferAllocator;

public class DrillClusterInfo {
  
  private static DrillClusterInfo instance;
  private DrillClient[] drillbits;
  private DrillConfig localConfig;

  BufferAllocator allocator = new DirectBufferAllocator();
  
  private DrillClusterInfo(){
    DrillClient client = new DrillClient();
    //TODO 配置文件支持
  }
  
  public static DrillClusterInfo getInstance() {
    return instance;
  }

  public DrillClient[] getDrillbits() {
    return drillbits;
  }

  public DrillConfig getLocalConfig() {
    if(localConfig == null){
      localConfig = DrillConfig.create();
    }
    return localConfig;
  }

  public BufferAllocator getAllocator() {
    return allocator;
  }
}
