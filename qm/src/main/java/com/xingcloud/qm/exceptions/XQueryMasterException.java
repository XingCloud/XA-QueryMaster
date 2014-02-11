package com.xingcloud.qm.exceptions;

/**
 * User: liuxiong
 * Date: 14-1-22
 * Time: 上午11:05
 */
public class XQueryMasterException extends Exception {

  public XQueryMasterException() {
    super();
  }

  public XQueryMasterException(String message) {
    super(message);
  }

  public XQueryMasterException(String message, Throwable cause) {
    super(message, cause);
  }

  public XQueryMasterException(Throwable cause) {
    super(cause);
  }

  protected XQueryMasterException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);
  }

}
