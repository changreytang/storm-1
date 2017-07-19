package storm.mesos.zookeeper;

import org.apache.commons.lang3.exception.ExceptionContext;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Created by rtang on 7/18/17.
 */
public class ZKClient {
  public static final Logger LOG = LoggerFactory.getLogger(ZKClient.class);
  private static ZooKeeper zk;
  private static ZKConnection zkConnection;

  // Default ZKClient for localhost
  public ZKClient() {
    try {
      zkConnection = new ZKConnection();
      zk = zkConnection.connect("localhost", 2000);
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  // connString is
  public ZKClient(String connString, int sessionTimeout) {
    try {
      zkConnection = new ZKConnection();
      zk = zkConnection.connect(connString, sessionTimeout);
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
  }

  public void closeConnection() {
    try {
      zkConnection.close();
    } catch (InterruptedException e) {
      LOG.error(e.getMessage());
    }
  }

  public void createNode(String path, byte[] data) throws KeeperException, InterruptedException  {
    zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
    LOG.info("New ZKNode created at: " + path);
  }

  public void updateNode(String path, byte[] data) throws KeeperException, InterruptedException {
    int version = zk.exists(path, true).getVersion();
    zk.setData(path, data, version);
  }

  public void deleteNode(String path) throws KeeperException, InterruptedException {
    int version = zk.exists(path, true).getVersion();
    zk.delete(path, version);
  }

  public boolean nodeExists(String path) throws KeeperException, InterruptedException {
    Stat stat = zk.exists(path, true);

    return stat != null;
  }

  public Stat getNodeStats(String path) throws KeeperException, InterruptedException {
    Stat stat = zk.exists(path, true);
    if (stat == null) {
      LOG.info("ZKNode doesn't exist at path: " + path);
    }
    return stat;
  }

  public String getNodeData(String path, boolean watchFlag) throws KeeperException, InterruptedException {
    try {
      Stat stat = getNodeStats(path);
      byte[] rawData = null;
      if (stat != null) {
        if (watchFlag) {
          ZKWatcher watch = new ZKWatcher();
          rawData = zk.getData(path, watch, null);
          watch.await();
        } else {
          rawData = zk.getData(path, null, null);
        }
        String data = new String(rawData, "UTF-8");
        return data;
      } else {
        LOG.info("ZKNode doesn't exist at path: " + path);
      }
    } catch (Exception e) {
      LOG.error(e.getMessage());
    }
    return null;
  }

}
