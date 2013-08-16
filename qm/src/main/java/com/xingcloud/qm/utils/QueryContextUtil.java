package com.xingcloud.qm.utils;

public class QueryContextUtil {

  private static String root = "";

  public static void setContextRoot(String path){
    root = path;
  }
  
  public static String getRoot(){
    return root;
  }
}
