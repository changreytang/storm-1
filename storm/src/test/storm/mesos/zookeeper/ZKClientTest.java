package storm.mesos.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 * Created by rtang on 7/18/17.
 */
public class ZKClientTest {
  /**
   * Testing target.
   */
  private final ZKClient target;

  /**
   * Setup testing target & sample data.
   */
  public ZKClientTest() {
    target = new ZKClient();
  }

  @Test
  public void createNode() {
    byte[] data = new byte[10];
    try {
      target.createNode("/test", data);
    } catch (Exception e) {
      assertTrue(false);
    }
    assertTrue(true);
  }

  @Test
  public void updateNode() {
  }

  @Test
  public void deleteNode() {
  }

  @Test
  public void nodeExists() {
  }

  @Test
  public void getNodeStats() {
  }

  @Test
  public void getNodeData() {
  }

}
