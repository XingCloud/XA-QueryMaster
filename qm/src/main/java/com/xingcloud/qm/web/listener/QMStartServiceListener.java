package com.xingcloud.qm.web.listener;

import com.xingcloud.qm.remote.QueryNode;
import org.apache.log4j.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class QMStartServiceListener implements ServletContextListener {

  private static final Logger LOGGER = Logger.getLogger(QMStartServiceListener.class);

  @Override
  public void contextDestroyed(ServletContextEvent arg0) {

  }

  @Override
  public void contextInitialized(ServletContextEvent arg0) {
    QueryNode.init();
    LOGGER.info("[STARTUP-LISTENER] - Query nodes inited.");
  }

}
