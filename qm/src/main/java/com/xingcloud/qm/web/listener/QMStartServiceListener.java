package com.xingcloud.qm.web.listener;

import com.xingcloud.qm.thread.BrokerESProvider;
import com.xingcloud.qm.thread.CachePutESProvider;
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
    BrokerESProvider.getInstance().init();
    LOGGER.info("[STARTUP-LISTENER] - Brokers have been inited.");
    CachePutESProvider.getInstance().init();
    LOGGER.info("[STARTUP-LISTENER] - Cache putters have been inited.");
  }

}
