package storm.mesos.zookeeper;

import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.ZooKeeper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;

/**
 * Created by rtang on 7/18/17.
 */
public class ZKConnection {
  public static final Logger LOG = LoggerFactory.getLogger(ZKConnection.class);
  private ZooKeeper zk;

  /**
   * connLatch is a synchronization aid that allows one or more threads to wait until a set of operations being
   * performed in other threads complete.
   */
  final CountDownLatch connLatch = new CountDownLatch(1);

  /**
   * connString is a comma separated string of separated host:port pairs, each corresponding to a zk server
   */
  public ZooKeeper connect(String connString, int sessionTimeout) throws IOException, InterruptedException {
    zk = new ZooKeeper(connString, sessionTimeout, new Watcher() {
      public void process(WatchedEvent e) {
        if (e.getState() == KeeperState.SyncConnected) {
          connLatch.countDown();
        }
      }
    });
    connLatch.await();
    LOG.info("ZooKeeper connection established");
    return zk;
  }

  public void close() throws InterruptedException {
    if (zk != null) {
      zk.close();
      LOG.info("ZooKeeper connection closed");
      return;
    }
    LOG.info("No ZooKeeper connection established, nothing to close");
  }
}
