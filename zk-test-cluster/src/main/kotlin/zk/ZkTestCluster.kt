package zk

import org.apache.curator.test.TestingCluster
import org.apache.zookeeper.Watcher
import org.apache.zookeeper.ZooKeeper
import kotlin.system.exitProcess


object ZkTestCluster {

    fun run(args: Array<String>) {

        if (args.isEmpty()) {
            println("Cluster size must be specified")
            exitProcess(-1)
        }

        val instanceQty = Integer.valueOf(args[0])
        var killedInstances = 0

        val cluster = TestingCluster(instanceQty)

        cluster.start()

        // Wait until servers start up properly and print the connectString
        val zooKeeper = ZooKeeper(cluster.connectString, 5000, Watcher { event ->
            if (event.state === Watcher.Event.KeeperState.SyncConnected) {
                println(cluster.connectString)
            }
        })

        do {
            val c = System.`in`.read().toChar()
            when (c) {
                'k' -> {
                    cluster.servers[killedInstances].close()
                    println("Server killed")
                    killedInstances++
                }
                'q' -> {
                    zooKeeper.close()
                    cluster.close()
                    println("Servers closed")
                    exitProcess(0)
                }
            }
        } while (c.toInt() != -1)
    }
}

fun main(args: Array<String>) = ZkTestCluster.run(args)
