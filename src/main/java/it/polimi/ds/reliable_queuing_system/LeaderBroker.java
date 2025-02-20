package it.polimi.ds.reliable_queuing_system;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * LeaderBroker:
 *  - Manages official queue data and read offsets.
 *  - Writes (CREATE, APPEND) and offset updates originate here.
 *  - Replicates data and offsets to followers.
 */
public class LeaderBroker implements BrokerNode {

    private final String brokerId;
    private final int port;
    private final QueueManager queueManager;
    private boolean running = false;

    // List of followers: Key = "host:port", Value = port (or combined follower info)
    private final Map<String, Integer> followers = new HashMap<>();

    public LeaderBroker(String brokerId, int port, QueueManager queueManager) {
        this.brokerId = brokerId;
        this.port = port;
        this.queueManager = queueManager;
    }

    @Override
    public void startBroker() {
        NetworkServer.startServer(this, port);
        running = true;
        System.out.println("LeaderBroker " + brokerId + " started on port " + port + ".");
    }

    @Override
    public void stopBroker() {
        NetworkServer.stopServer(port);
        running = false;
    }

    @Override
    public boolean isLeader() {
        return true;
    }

    /**
     * Registers a follower so the leader can replicate queue data and offset changes.
     */
    public void registerFollower(String followerHost, int followerPort) {
        String key = followerHost + ":" + followerPort;
        if (!followers.containsKey(key)) {
            followers.put(key, followerPort);
            System.out.println("Follower registered: " + key);
        }
    }

    @Override
    public void createQueue(String queueName) {
        queueManager.createQueue(queueName);
        System.out.println("Leader created queue [" + queueName + "]");
    }

    @Override
    public void appendData(String queueName, int data) {
        queueManager.appendData(queueName, data);
        System.out.println("Leader appended data " + data + " to queue [" + queueName + "]");
        // Replicate to followers
        replicateDataToFollowers(queueName, data);
    }

    /**
     * - The leader increments the offset for (queueName, clientId) and returns the data item.
     * - Then replicates the offset to followers so that they remain up-to-date.
     */
    @Override
    public Integer readData(String queueName, String clientId) {
        // Use queueManager to read next item, which increments the stored offset
        Integer item = queueManager.readNext(queueName, clientId);
        // Replicate offset after reading
        int newOffset = queueManager.getClientOffset(queueName, clientId);
        replicateOffsetToFollowers(queueName, clientId, newOffset);
        return item;
    }

    @Override
    public Map<String, List<Integer>> getAllQueueData() {
        return queueManager.getAllQueuesSnapshot();
    }

    @Override
    public void replicateDataToFollowers(String queueName, int data) {
        // For each registered follower, push "REPLICATE|queueName|data"
        for (String fKey: followers.keySet()) {
            String[] parts = fKey.split(":");
            String followerHost = parts[0];
            int followerPort = Integer.parseInt(parts[1]);
            NetworkClient.sendRequest(
                    followerHost,
                    followerPort,
                    "REPLICATE" + NetworkUtils.MSG_SEPARATOR + queueName + NetworkUtils.MSG_SEPARATOR + data
            );
        }
    }

    @Override
    public void replicateOffsetToFollowers(String queueName, String clientId, int newOffset) {
        // For each registered follower, push "UPDATEOFFSET|queueName|clientId|newOffset"
        for (String fKey: followers.keySet()) {
            String[] parts = fKey.split(":");
            String followerHost = parts[0];
            int followerPort = Integer.parseInt(parts[1]);
            NetworkClient.sendRequest(
                    followerHost,
                    followerPort,
                    "UPDATEOFFSET" + NetworkUtils.MSG_SEPARATOR + queueName + NetworkUtils.MSG_SEPARATOR
                            + clientId + NetworkUtils.MSG_SEPARATOR + newOffset
            );
        }
    }

    /**
     * Returns local offset from the queue manager for the given queue and client.
     */
    @Override
    public int fetchClientOffset(String queueName, String clientId) {
        return queueManager.getClientOffset(queueName, clientId);
    }

    /**
     * Updates the offset for (queueName, clientId) in the queue manager.
     */
    @Override
    public void updateClientOffset(String queueName, String clientId, int newOffset) {
        queueManager.setClientOffset(queueName, clientId, newOffset);
    }
}