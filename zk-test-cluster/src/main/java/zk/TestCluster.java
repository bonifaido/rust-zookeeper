package zk;

import org.apache.curator.test.TestingCluster;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

/**
 *
 */
public class TestCluster {
    public static void main(String[] args) throws Exception {
        final TestingCluster cluster = new TestingCluster(2);

        cluster.start();

        // Wait until servers start up properly
        new ZooKeeper(cluster.getConnectString(), 5000, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
                if (event.getState() == Event.KeeperState.SyncConnected) {
                    System.out.println(cluster.getConnectString());
                }
            }
        });

        synchronized (cluster) {
            cluster.wait();
        }

        cluster.close();
    }
}
