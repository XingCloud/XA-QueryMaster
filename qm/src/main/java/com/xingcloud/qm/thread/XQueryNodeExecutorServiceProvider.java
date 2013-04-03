package com.xingcloud.qm.thread;

import static com.xingcloud.qm.utils.QueryMasterConstant.ESI_MAP;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;

public class XQueryNodeExecutorServiceProvider {
    private static final Logger LOGGER = Logger
            .getLogger(XQueryNodeExecutorServiceProvider.class);
    private static ExecutorService service;

    static {
        ExecutorServiceInfo esi = ESI_MAP.get("QUERY-NODE");
        synchronized (XQueryNodeExecutorServiceProvider.class) {
            service = Executors.newFixedThreadPool(esi.getThreadCount(),
                    new XThreadFactory(esi.getName()));
            LOGGER.info("[THREAD-POOL] - " + esi.getName() + " inited.");
        }
    }

    public static void destory() {
        try {
            LOGGER.info("Shutdown ExecutorService");
            int time = 3;
            if (service != null) {
                service.shutdown();
                LOGGER.info("Waiting unfinished thread for " + time
                        + " seconds");
                service.awaitTermination(time, TimeUnit.SECONDS);
            }
        } catch (InterruptedException e) {
            LOGGER.error("counld not wait");
        } finally {
            LOGGER.info("shutdown now");
            if (service != null) {
                service.shutdownNow();
            }
        }
    }

    public static ExecutorService getService() {
        return service;
    }

}
