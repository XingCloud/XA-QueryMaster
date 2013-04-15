package com.xingcloud.qm.web.listener;

import com.xingcloud.basic.mail.XMailService;
import com.xingcloud.qm.thread.BrokerESProvider;
import com.xingcloud.qm.thread.CachePutESProvider;
import com.xingcloud.qm.thread.WorkerESProvider;
import org.apache.log4j.Logger;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

public class QMShutDownListener implements ServletContextListener {
  private static final Logger LOGGER = Logger.getLogger(QMShutDownListener.class);

  public void contextDestroyed(ServletContextEvent arg0) {
    XMailService.destroy();
    LOGGER.info("[SHUTDOWN-LISTENER] - Xmail service is shutdown.");
    CachePutESProvider.getInstance().destory();
    LOGGER.info("[SHUTDOWN-LISTENER] - Cache putters are shutdown.");
    WorkerESProvider.getInstance().destory();
    LOGGER.info("[SHUTDOWN-LISTENER] - Workers are shutdown.");
    BrokerESProvider.getInstance().destory();
    LOGGER.info("[SHUTDOWN-LISTENER] - Brokers are shutdown.");
  }

  public void contextInitialized(ServletContextEvent arg0) {
  }

}
