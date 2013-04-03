package com.xingcloud.qm.queue;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.lang3.RandomStringUtils;

public class QueueContainer {

    private static QueueContainer container;

    private QueueContainer() {
        this.capacity = 10;
        this.submitQueue = new LinkedBlockingQueue<QueryJob>();
        this.workingQueue = new LinkedBlockingQueue<QueryJob>(this.capacity);
        startTransfer();
    }

    public static synchronized QueueContainer getInstance() {
        if (container == null) {
            container = new QueueContainer();
        }
        return container;
    }

    private final LinkedBlockingQueue<QueryJob> submitQueue;

    private final LinkedBlockingQueue<QueryJob> workingQueue;

    private final int capacity;

    private void startTransfer() {
        Transferer t = new Transferer();
        Thread transferThread = new Thread(t, "TransferThread");
        transferThread.start();
    }

    public void put( QueryJob task ) throws InterruptedException {
        submitQueue.put(task);
    }

    public QueryJob take() throws InterruptedException {
        return workingQueue.take();
    }

    public synchronized List<QueryJob> listPreparedTask() {
        List<QueryJob> l = new ArrayList<QueryJob>(submitQueue);
        return l;
    }

    private class Transferer implements Runnable {

        @Override
        public void run() {
            QueryJob task = null;
            try {
                while (true) {

                    task = submitQueue.take();
                    if (task == null) {
                        continue;
                    }
                    workingQueue.put(task);
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            } finally {
                Thread.currentThread().interrupt();
            }
        }
    }
}
