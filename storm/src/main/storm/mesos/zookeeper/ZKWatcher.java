package storm.mesos.zookeeper;

import org.apache.zookeeper.AsyncCallback.StatCallback;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

/**
 * Created by rtang on 7/18/17.
 */
public class ZKWatcher implements Watcher, StatCallback {
  public static final Logger LOG = LoggerFactory.getLogger(ZKWatcher.class);
  CountDownLatch countDownLatch;

  public ZKWatcher() {
    countDownLatch = new CountDownLatch(1);
  }

  @Override
  public void processResult(int i, String s, Object o, Stat stat) {
  }

  @Override
  public void process(WatchedEvent watchedEvent) {
    LOG.info("Event Path: " + watchedEvent.getPath() +
            "\nEvent State: " + watchedEvent.getState() +
            "\nEvent Type: " + watchedEvent.getType());
    countDownLatch.countDown();
  }

  public void await() throws InterruptedException {
    countDownLatch.await();
  }

}
