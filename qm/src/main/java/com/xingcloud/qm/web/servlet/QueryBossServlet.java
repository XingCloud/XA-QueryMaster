package com.xingcloud.qm.web.servlet;

import com.caucho.hessian.server.HessianServlet;
import com.xingcloud.qm.queue.QueryJob;
import com.xingcloud.qm.queue.QueueContainer;
import com.xingcloud.qm.service.Submit;

public class QueryBossServlet extends HessianServlet implements Submit {
    private static final long serialVersionUID = 5320413547601487799L;

    @Override
    public boolean submit( String sql ) {
        long timestamp = System.currentTimeMillis();
        QueueContainer qc = QueueContainer.getInstance();
        try {
            qc.put(new QueryJob(sql, timestamp));
            return true;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

    @Override
    public boolean submit( String sql, String cacheKey ) {
        long timestamp = System.currentTimeMillis();
        QueueContainer qc = QueueContainer.getInstance();
        try {
            qc.put(new QueryJob(cacheKey, sql, timestamp));
            return true;
        } catch (InterruptedException e) {
            e.printStackTrace();
            return false;
        }
    }

}
