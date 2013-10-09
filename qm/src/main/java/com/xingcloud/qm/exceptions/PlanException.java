package com.xingcloud.qm.exceptions;

/**
 * User: Z J Wu Date: 13-8-5 Time: 下午3:25 Package: com.xingcloud.webinterface.exception
 */
public class PlanException extends Exception {
  public PlanException() {
  }

  public PlanException(String message) {
    super(message);
  }

  public PlanException(String message, Throwable cause) {
    super(message, cause);
  }

  public PlanException(Throwable cause) {
    super(cause);
  }
}
