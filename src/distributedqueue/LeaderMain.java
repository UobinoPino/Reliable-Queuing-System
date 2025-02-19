package distributedqueue;

/**
 * Launches a single LeaderBroker in the "distributedqueue" package.
 * The BrokerCluster is used here to manage and start the broker.
 * This class should be run on one machine (notebook) in your LAN.
 */
public class LeaderMain {

    public static void main(String[] args) {
        // 1. Create a QueueManager for stable storage of queues and offsets.
        QueueManager queueManager = new QueueManager();

        // 2. Instantiate the LeaderBroker on a designated port (e.g., 5001).
        LeaderBroker leader = new LeaderBroker("leader1", 5001, queueManager);

        // 3. Create the BrokerCluster and register the leader as part of it.
        BrokerCluster cluster = new BrokerCluster();
        cluster.addBroker(leader);  // This automatically sets this broker as leader.

        // 4. Start the cluster (which starts the leader listening on port 5001).
        cluster.startAllBrokers();
        System.out.println("Leader broker started on port 5001.");

        // 5. Keep the application running to maintain the leader's availability.
        while (true) {
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}