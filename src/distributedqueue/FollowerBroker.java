package distributedqueue;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Follower broker implementation.
 *
 * Receives replicated data from the leader and can handle reading for clients.
 *
 * Requirements addressed:
 * 1 (follower), 4 (replication from leader), 6 (clients can read from follower),
 * 7 (track offsets), 11 (stable storage), 12 (recovery with leader),
 * 14 (persist offsets), 15 (client reconnect).
 *
 * Networking:
 *  - The follower also starts a server socket and awaits requests (create/read can be forwarded to leader if needed).
 */
public class FollowerBroker implements BrokerNode {

    private final String brokerId;
    private final AtomicBoolean running;
    private final Map<String, List<Integer>> queues;   // queueName -> data
    private final Map<String, Map<String, Integer>> clientOffsets; // queueName -> (clientId -> offset)
    private final QueueManager queueManager;
    private final String leaderHost;
    private final int leaderPort;
    private final int brokerPort;

    /**
     * @param brokerId ID for this follower broker.
     * @param brokerPort Port on which it will listen for client or leader requests.
     * @param leaderHost The hostname/IP address of the leader.
     * @param leaderPort The port of the leader's server socket.
     * @param queueManager Stable storage manager.
     */
    public FollowerBroker(String brokerId, int brokerPort, String leaderHost, int leaderPort, QueueManager queueManager) {
        this.brokerId = brokerId;
        this.brokerPort = brokerPort;
        this.leaderHost = leaderHost;
        this.leaderPort = leaderPort;
        this.queueManager = queueManager;
        this.running = new AtomicBoolean(false);
        this.queues = new ConcurrentHashMap<>();
        this.clientOffsets = new ConcurrentHashMap<>();
    }

    @Override
    public void startBroker() {
        running.set(true);
        // Start networking server
        NetworkServer.startServer(this, brokerPort);
        // Register to the leader that we exist, so the leader can replicate
        NetworkClient.registerFollower(leaderHost, leaderPort, NetworkUtils.getLocalHostAddress(), brokerPort);
        System.out.println("FollowerBroker " + brokerId + " started on port " + brokerPort + ".");
    }

    @Override
    public void stopBroker() {
        running.set(false);
        NetworkServer.stopServer(brokerPort);
        System.out.println("FollowerBroker " + brokerId + " stopped.");
    }

    @Override
    public synchronized void createQueue(String queueName) {
        // Only leader should do actual creation. Optionally, we can forward the request:
        System.out.println("Follower " + brokerId + " forwarding createQueue to leader: " + leaderHost + ":" + leaderPort);
        NetworkClient.sendCreateQueueRequest(leaderHost, leaderPort, queueName);
    }

    @Override
    public synchronized void appendData(String queueName, int data) {
        // Only leader should do actual append. Optionally, forward it:
        System.out.println("Follower " + brokerId + " forwarding appendData to leader: " + leaderHost + ":" + leaderPort);
        NetworkClient.sendAppendDataRequest(leaderHost, leaderPort, queueName, data);
    }

    @Override
    public synchronized Integer readData(String queueName, String clientId) {
        if (!queues.containsKey(queueName)) {
            return null;
        }
        Map<String, Integer> offsets = clientOffsets.get(queueName);
        if (!offsets.containsKey(clientId)) {
            offsets.put(clientId, 0);
        }
        int offset = offsets.get(clientId);
        List<Integer> dataList = queues.get(queueName);
        if (offset < dataList.size()) {
            int value = dataList.get(offset);
            offsets.put(clientId, offset + 1);
            persistClientOffset(queueName, clientId, offset + 1);
            return value;
        }
        return null;
    }

    /**
     * For replication from the leader to this follower.
     */
    @Override
    public synchronized void replicateDataToFollowers(String queueName, int newData) {
        if (!queues.containsKey(queueName)) {
            queues.put(queueName, new ArrayList<>());
            clientOffsets.put(queueName, new ConcurrentHashMap<>());
            queueManager.createQueueOnDisk(queueName);
        }
        queues.get(queueName).add(newData);
        queueManager.appendDataToDisk(queueName, newData);
    }

    @Override
    public void synchronizeWithLeader() {
        // Pull data from the leader. The simplest approach is a full snapshot:
        Map<String, List<Integer>> leaderData = NetworkClient.requestFullDataSnapshot(leaderHost, leaderPort);
        if (leaderData == null) {
            System.out.println("Follower " + brokerId + " failed to get snapshot from leader.");
            return;
        }
        for (Map.Entry<String, List<Integer>> entry : leaderData.entrySet()) {
            String queueName = entry.getKey();
            List<Integer> dataList = entry.getValue();
            if (!queues.containsKey(queueName)) {
                queues.put(queueName, new ArrayList<>());
                clientOffsets.put(queueName, new ConcurrentHashMap<>());
                queueManager.createQueueOnDisk(queueName);
            }
            List<Integer> localList = queues.get(queueName);
            // Fill in any missing data
            for (int i = localList.size(); i < dataList.size(); i++) {
                localList.add(dataList.get(i));
                queueManager.appendDataToDisk(queueName, dataList.get(i));
            }
        }
    }

    @Override
    public boolean isLeader() {
        return false;
    }

    @Override
    public void persistClientOffset(String queueName, String clientId, int offset) {
        queueManager.persistClientOffsetToDisk(queueName, clientId, offset);
    }

    @Override
    public Map<String, List<Integer>> getAllQueueData() {
        return queues;
    }

    @Override
    public int getBrokerPort() {
        return brokerPort;
    }
}