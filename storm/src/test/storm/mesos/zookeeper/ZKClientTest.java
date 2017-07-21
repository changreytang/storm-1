package storm.mesos.zookeeper;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;
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
    target = new ZKClient("192.168.20.121:2181",2000);
  }

  @Test
  public void testCreateNodeThenDelete() {
    byte[] data = new byte[10];
    try {
      target.createNode("/test1", data);
    } catch (Exception e) {
      assertTrue("Couldn't create node", false);
    }
    try {
      target.deleteNode("/test1");
    } catch (Exception e) {
      assertTrue("Couldn't delete node", false);
    }
    assertTrue(true);
  }

  @Test
  public void testCreateNodeCheckExistenceThenDelete() {
    byte[] data = new byte[10];
    try {
      target.createNode("/test2", data);
    } catch (Exception e) {
      assertTrue("Couldn't create node", false);
    }
    try {
      assertTrue("Node created but doesn't exist", target.nodeExists("/test2"));
    } catch (Exception e) {
      assertTrue("nodeExists method throwing exception: " + e.toString(), false);
    }
    try {
      target.deleteNode("/test2");
    } catch (Exception e) {
      assertTrue("Couldn't delete node", false);
    }
    assertTrue(true);
  }

  @Test
  public void testCheckExistenceOfNonexistentNode() {
    try {
      assertFalse("Nonexistent node exists for some reason", target.nodeExists("/test3"));
    } catch (Exception e) {
      assertTrue("nodeExists method throwing exception: " + e.toString(), false);
    }
  }

  @Test
  public void testCreateNodeGetStatsThenDelete() {
    byte[] data = new byte[10];
    try {
      target.createNode("/test4", data);
    } catch (Exception e) {
      assertTrue("Couldn't create node", false);
    }
    Stat stat = null;
    try {
      stat = target.getNodeStats("/test4");
    } catch (Exception e) {
      assertTrue("getNodeStats method throwing exception: " + e.toString(), false);
    }
    if(stat == null) {
      assertTrue("No Stat object returned from node indicating it might not exist", false);
    }
    try {
      target.deleteNode("/test4");
    } catch (Exception e) {
      assertTrue("Couldn't delete node", false);
    }
    assertTrue(true);
  }

  @Test
  public void testCreateNodeGetDataThenDelete() {
    String initialString = "test";
    byte[] data = initialString.getBytes();
    try {
      target.createNode("/test5", data);
    } catch (Exception e) {
      try {
        target.deleteNode("/test5");
      } catch (Exception ex) {
        assertTrue("Couldn't delete node", false);
      }
      assertTrue("Couldn't create node", false);
    }
    String returnedString = "";
    try {
      returnedString = target.getNodeData("/test5", false);
    } catch (Exception e) {
      assertTrue("getNodeData method throwing exception: " + e.toString(), false);
    }
    assertTrue("Data retrieved doesn't match initial data", initialString.equals(returnedString));
    try {
      target.deleteNode("/test5");
    } catch (Exception e) {
      assertTrue("Couldn't delete node", false);
    }
    assertTrue(true);
  }

  @Test
  public void testCreateNodeUpdateDataThenDelete() {
    String initialString = "test";
    byte[] data = initialString.getBytes();

    /** Create initial node */
    try {
      target.createNode("/test6", data);
    } catch (Exception e) {
      /** Try to delete node if it already exists, could be reason by client couldn't create node */
      try {
        target.deleteNode("/test6");
      } catch (Exception ex) {
        assertTrue("Couldn't delete node", false);
      }
      assertTrue("Couldn't create node", false);
    }

    /** Update data on created node */
    String updatedString = "updated";
    byte[] updatedData = updatedString.getBytes();
    try {
      target.updateNode("/test6", updatedData);
    } catch (Exception e) {
      assertTrue("Couldn't update node data", false);
    }

    /** Check if data was updated correct on node by retrieving it */
    String returnedString = "";
    try {
      returnedString = target.getNodeData("/test6", false);
    } catch (Exception e) {
      assertTrue("getNodeData method throwing exception: " + e.toString(), false);
    }
    assertTrue("Data retrieved doesn't match updated data", updatedString.equals(returnedString));

    /** Delete the node so we can run this test again */
    try {
      target.deleteNode("/test6");
    } catch (Exception e) {
      assertTrue("Couldn't delete node", false);
    }
    assertTrue(true);
  }
}
