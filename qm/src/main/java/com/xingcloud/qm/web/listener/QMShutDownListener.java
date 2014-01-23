package com.xingcloud.qm.web.listener;

import org.apache.log4j.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class QMShutDownListener implements ServletContextListener {
  private static final Logger LOGGER = Logger.getLogger(QMShutDownListener.class);

  public void contextDestroyed(ServletContextEvent arg0) {
  }

  public void contextInitialized(ServletContextEvent arg0) {
  }

}
