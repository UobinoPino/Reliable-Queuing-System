package distributedqueue;

import java.util.List;
import java.util.Map;

/**
 * FollowerBroker:
 *  - Holds replicated queue data from leader.
 *  - For read requests from clients, this updated design:
 *       1) Sends the read request to the leader to get the correct next item.
 *       2) The leader reads and updates offset, replicates offset to all brokers.
 *       3) Follower receives offset update, stores it, and returns the item from the leader to the client.
 *  - For append requests from clients, this updated design:
 *  *       1) Forwards the append request to the leader via "FORWARDED_APPEND".
 *  *       2) Leader performs the actual append and replicates to all followers.
 */
public class FollowerBroker implements BrokerNode {

    private final String brokerId;
    private final int port;
    private final String leaderHost;
    private final int leaderPort;
    private final QueueManager queueManager;
    private boolean running = false;

    public FollowerBroker(String brokerId, int port, String leaderHost, int leaderPort, QueueManager queueManager) {
        this.brokerId = brokerId;
        this.port = port;
        this.leaderHost = leaderHost;
        this.leaderPort = leaderPort;
        this.queueManager = queueManager;
    }

    @Override
    public void startBroker() {
        NetworkServer.startServer(this, port);
        running = true;
        // Register with the leader
        NetworkClient.registerFollower(leaderHost, leaderPort, NetworkUtils.getLocalHostAddress(), port);
        System.out.println("FollowerBroker " + brokerId + " started on port " + port + ".");
    }

    @Override
    public void stopBroker() {
        running = false;
        NetworkServer.stopServer(port);
    }

    @Override
    public boolean isLeader() {
        return false;
    }

    @Override
    public void createQueue(String queueName) {
        // Typically only the leader handles the official create,
        // but we allow local creation if replicate calls haven't arrived yet
        queueManager.createQueue(queueName);
    }

    /**
     * On a follower, the updated logic is:
     *   1) Forward the append request to the leader using "FORWARDED_APPEND|queueName|data".
     *   2) The leader appends it and replicates to all followers.
     *   3) This follower eventually receives the replication call and updates its local state.
     */
    @Override
    public void appendData(String queueName, int data) {
        String request = "FORWARDED_APPEND"
                + NetworkUtils.MSG_SEPARATOR + queueName
                + NetworkUtils.MSG_SEPARATOR + data;
        NetworkClient.sendRequest(leaderHost, leaderPort, request);
    }

    /**
     * In this revised approach, the follower:
     *   1) Sends a "FORWARDED_READ" request to the leader.
     *   2) Leader returns the next data item as a string.
     *   3) The leader also updates and replicates the offset among all brokers.
     *   4) When the follower receives offset updates, it adjusts the local store accordingly.
     */
    @Override
    public Integer readData(String queueName, String clientId) {
        // Forward read to leader
        String request = "FORWARDED_READ" + NetworkUtils.MSG_SEPARATOR + queueName + NetworkUtils.MSG_SEPARATOR + clientId;
        String response = NetworkClient.sendRequest(leaderHost, leaderPort, request);

        // If the leader returns "null", that means no data
        if (response.equals("null")) {
            return null;
        }
        try {
            return Integer.valueOf(response);
        } catch (NumberFormatException e) {
            // If there's an error or unknown response, return null
            return null;
        }
    }

    @Override
    public Map<String, List<Integer>> getAllQueueData() {
        return queueManager.getAllQueuesSnapshot();
    }

    /**
     * Called by the leader when the leader replicates data to this follower.
     */
    @Override
    public void replicateDataToFollowers(String queueName, int data) {
        // Store it locally
        queueManager.appendData(queueName, data);
    }

    /**
     * Called by the leader when the leader replicates offset changes to this follower.
     */
    @Override
    public void replicateOffsetToFollowers(String queueName, String clientId, int newOffset) {
        // Not used directly in the follower for replication calls;
        // see "UPDATEOFFSET" command in NetworkServer for how the follower updates locally.
    }

    @Override
    public int fetchClientOffset(String queueName, String clientId) {
        // Follower typically doesn’t supply offset data to others; returns local offset
        return queueManager.getClientOffset(queueName, clientId);
    }

    @Override
    public void updateClientOffset(String queueName, String clientId, int newOffset) {
        // We set the local offset from the replication calls
        queueManager.setClientOffset(queueName, clientId, newOffset);
    }
}