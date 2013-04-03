package com.xingcloud.qm.web.listener;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;

import org.apache.log4j.Logger;

import com.xingcloud.basic.conf.ConfigReader;
import com.xingcloud.basic.conf.Dom;
import com.xingcloud.qm.redis.CachePutter;
import com.xingcloud.qm.service.QueryBroker;

public class QMStartServiceListener implements ServletContextListener {

    private static final Logger LOGGER = Logger
            .getLogger(QMStartServiceListener.class);

    @Override
    public void contextDestroyed( ServletContextEvent arg0 ) {

    }

    @Override
    public void contextInitialized( ServletContextEvent arg0 ) {
        Dom root = ConfigReader.getDom("common.xml");
        Dom brokerDom = root.element("broker");

        int count = Integer.valueOf(brokerDom.getAttributeValue("count"));

        QueryBroker qb = new QueryBroker();
        String threadName = null;
        for( int i = 0; i < count; i++ ) {
            threadName = "QueryBrokerThread" + (i + 1);
            new Thread(qb, threadName).start();
            LOGGER.info("[QM-SYS-VAR] - Broker thread started - " + threadName);
        }

        CachePutter cp = new CachePutter();
        Dom cachePutterDom = root.element("cache-putter");
        count = Integer.valueOf(cachePutterDom.getAttributeValue("count"));
        for( int i = 0; i < count; i++ ) {
            threadName = "CachePutterThread" + (i + 1);
            new Thread(cp, threadName).start();
            LOGGER.info("[QM-SYS-VAR] - Cache putter thread started - "
                    + threadName);
        }
    }

}
