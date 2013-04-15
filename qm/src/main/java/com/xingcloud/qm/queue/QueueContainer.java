package com.xingcloud.qm.queue;

import static com.xingcloud.qm.utils.QueryMasterVariables.ESI_MAP;

import com.xingcloud.basic.concurrent.ExecutorServiceInfo;
import org.apache.log4j.Logger;

import java.util.concurrent.LinkedBlockingQueue;

public class QueueContainer {

  private static final Logger LOGGER = Logger.getLogger(QueueContainer.class);
  private static QueueContainer container;

  private QueueContainer() {
    ExecutorServiceInfo esi = ESI_MAP.get("QUERY-BROKER");
    this.capacity = esi.getThreadCount();
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
    final int capacityOfQueue = this.capacity;
    Runnable r = new Runnable() {
      @Override public void run() {
        QueryJob task;
        try {
          while (true) {
            task = submitQueue.take();
            if (task == null) {
              continue;
            }
            workingQueue.put(task);
          }
        } catch (InterruptedException e) {
          LOGGER.info("[JOB-QUEUE] - Transfer thread will be shutdown(" +
                          Thread.currentThread().getName() + ").");
          Thread.currentThread().interrupt();
        }
      }
    };
    Thread transferThread = new Thread(r, "QueryTransferThread");
    transferThread.start();
    LOGGER.info("[JOB-QUEUE] - Transfer thread started(" + transferThread
        .getName() + "), capacity of working queue is " + capacityOfQueue + ".");
  }

  public boolean submit(QueryJob queryJob) throws InterruptedException {
    if (submitQueue.contains(queryJob)) {
      return false;
    }
    submitQueue.put(queryJob);
    return true;
  }

  public QueryJob fetchOne() throws InterruptedException {
    return workingQueue.take();
  }

}
