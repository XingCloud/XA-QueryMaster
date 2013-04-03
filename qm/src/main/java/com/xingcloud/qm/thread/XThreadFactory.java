package com.xingcloud.qm.thread;

import static com.xingcloud.qm.utils.QueryMasterConstant.THREAD_PREFIX;

import java.text.DecimalFormat;
import java.util.UUID;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

public class XThreadFactory implements ThreadFactory {
    private final String id;
    static final AtomicInteger poolNumber = new AtomicInteger(1);
    final ThreadGroup group;
    final AtomicInteger threadNumber = new AtomicInteger(1);
    final String namePrefix;

    public XThreadFactory(String threadNamePrefix) {
        id = UUID.randomUUID().toString();
        SecurityManager s = System.getSecurityManager();
        group = (s != null) ? s.getThreadGroup() : Thread.currentThread()
                .getThreadGroup();
        if (threadNamePrefix == null || threadNamePrefix.isEmpty()) {
            namePrefix = THREAD_PREFIX + "." + poolNumber.getAndIncrement()
                    + ".";
        } else {
            namePrefix = threadNamePrefix + ".";
        }
    }

    public Thread newThread( Runnable r ) {
        int tn = threadNumber.getAndIncrement();
        DecimalFormat df = new DecimalFormat("0000");
        Thread t = new Thread(group, r, namePrefix + df.format(tn), 0);
        if (!t.isDaemon())
            t.setDaemon(true);
        if (t.getPriority() != Thread.NORM_PRIORITY)
            t.setPriority(Thread.NORM_PRIORITY);
        return t;
    }

    @Override
    public String toString() {
        return "XQueryThreadFactory." + id + "." + threadNumber;
    }

    public static void main( String[] args ) {
        XThreadFactory f = new XThreadFactory("a");
        System.out.println(f);
    }

}
