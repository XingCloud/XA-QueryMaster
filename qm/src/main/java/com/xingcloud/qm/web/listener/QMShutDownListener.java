package com.xingcloud.qm.web.listener;

import com.xingcloud.basic.mail.XMailService;
import org.apache.log4j.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class QMShutDownListener implements ServletContextListener {
  private static final Logger LOGGER = Logger.getLogger(QMShutDownListener.class);

  public void contextDestroyed(ServletContextEvent arg0) {
    XMailService.destroy();
    LOGGER.info("[SHUTDOWN-LISTENER] - Xmail service is shutdown.");
  }

  public void contextInitialized(ServletContextEvent arg0) {
  }

}
