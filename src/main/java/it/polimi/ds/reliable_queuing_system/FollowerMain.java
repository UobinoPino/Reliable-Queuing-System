package it.polimi.ds.reliable_queuing_system;

/**
 * Launches a single FollowerBroker in the "distributedqueue" package.
 * The Follower will automatically register with the Leader.
 * This class should be run on another machine (notebook) in your LAN.
 */
public class FollowerMain {

    public static void main(String[] args) {
        // 1. Create a QueueManager for stable storage of queues and offsets.
        QueueManager queueManager = new QueueManager();

        // 2. Enter the IP/hostname of the Leader node and its port.
        //    Example: "192.168.1.10" and 5001 (adjust to match your leader's machine).
       // String leaderHost = "192.168.1.10"; se non da stesso pc
        String leaderHost = "127.0.0.1";
        int leaderPort = 5001;

        // 3. Decide the local port on which this follower listens (e.g., 5002).
        FollowerBroker follower = new FollowerBroker(
                "follower1",
                5002,
                leaderHost,
                leaderPort,
                queueManager
        );

        // 4. Optionally add to a local BrokerCluster (not strictly required for a single follower).
        BrokerCluster cluster = new BrokerCluster();
        cluster.addBroker(follower);

        // 5. Start the cluster, which starts the follower listening on port 5002,
        //    and registers with the leader at leaderHost:leaderPort.
        cluster.startAllBrokers();
        System.out.println("Follower broker started on port 5002. Leader at " + leaderHost + ":" + leaderPort);

        // 6. Keep this application alive to maintain the follower.
        while (true) {
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}