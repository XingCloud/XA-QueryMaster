package com.xingcloud.qm.service;

import org.apache.drill.exec.client.DrillClient;

public class DrillClusterInfo {
  
  private static DrillClusterInfo instance;
  private DrillClient[] drillbits;

  public static DrillClusterInfo getInstance() {
    return instance;
  }

  public DrillClient[] getDrillbits() {
    return drillbits;
  }
}
