package com.xingcloud.qm.web.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.Logger;

import com.xingcloud.qm.thread.XQueryNodeExecutorServiceProvider;

public class QMShutDownListener implements ServletContextListener {
    private static final Logger LOGGER = Logger
            .getLogger(QMShutDownListener.class);

    public void contextDestroyed( ServletContextEvent arg0 ) {
        LOGGER.info("Shutdown QueryNode-ExecutorService");
        XQueryNodeExecutorServiceProvider.destory();
    }

    public void contextInitialized( ServletContextEvent arg0 ) {

    }

}
