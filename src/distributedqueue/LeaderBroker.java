package distributedqueue;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Leader broker implementation.
 *
 * Handles:
 *  - Queue creation
 *  - Data append
 *  - Replication to followers
 *  - Client read offsets
 *
 * Requirements addressed:
 * 1 (leader), 4 (replication), 5 (client connections to leader for writes),
 * 7 (client offsets), 11 (stable storage), 12 (recovery sync from leader side if needed),
 * 14 (persist client offsets), 15 (reconnect logic can query the offset).
 *
 * Networking:
 *  - This broker can accept remote calls (via NetworkServer) or send replication calls to followers.
 */
public class LeaderBroker implements BrokerNode {

    private final String brokerId;
    private final AtomicBoolean running;
    private final Map<String, List<Integer>> queues;   // queueName -> data
    private final Map<String, Map<String, Integer>> clientOffsets; // queueName -> (clientId -> offset)
    private final List<FollowerInfo> followers;
    private final QueueManager queueManager;
    private final int brokerPort;

    /**
     * @param brokerId Identifier for this leader.
     * @param brokerPort The port on which this leader will open a server socket.
     * @param queueManager Manages stable storage.
     */
    public LeaderBroker(String brokerId, int brokerPort, QueueManager queueManager) {
        this.brokerId = brokerId;
        this.brokerPort = brokerPort;
        this.queueManager = queueManager;
        this.running = new AtomicBoolean(false);
        this.queues = new ConcurrentHashMap<>();
        this.clientOffsets = new ConcurrentHashMap<>();
        this.followers = Collections.synchronizedList(new ArrayList<>());
    }

    @Override
    public void startBroker() {
        running.set(true);
        // Start the networking portion
        NetworkServer.startServer(this, brokerPort);
        System.out.println("LeaderBroker " + brokerId + " started on port " + brokerPort + ".");
    }

    @Override
    public void stopBroker() {
        running.set(false);
        NetworkServer.stopServer(brokerPort);
        System.out.println("LeaderBroker " + brokerId + " stopped.");
    }

    @Override
    public synchronized void createQueue(String queueName) {
        if (!queues.containsKey(queueName)) {
            queues.put(queueName, new ArrayList<>());
            clientOffsets.put(queueName, new ConcurrentHashMap<>());
            queueManager.createQueueOnDisk(queueName);
            System.out.println("Queue created on leader: " + queueName);
        }
    }

    @Override
    public synchronized void appendData(String queueName, int data) {
        if (queues.containsKey(queueName)) {
            queues.get(queueName).add(data);
            // Persist locally
            queueManager.appendDataToDisk(queueName, data);
            // Replicate to followers
            replicateDataToFollowers(queueName, data);
        } else {
            System.out.println("Queue does not exist: " + queueName);
        }
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

    @Override
    public void replicateDataToFollowers(String queueName, int newData) {
        synchronized (followers) {
            for (FollowerInfo follower : followers) {
                // Send a replication request to each follower
                NetworkClient.sendReplicationRequest(follower.getHost(), follower.getPort(), queueName, newData);
            }
        }
    }

    @Override
    public void synchronizeWithLeader() {
        // Leader doesn't need to sync with itself
    }

    @Override
    public boolean isLeader() {
        return true;
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

    /**
     * A leader can register a new follower so that replication can be triggered.
     * @param followerHost The IP/hostname of the follower broker.
     * @param followerPort The port on which the follower is listening.
     */
    public void registerFollower(String followerHost, int followerPort) {
        // Possibly avoid duplicates
        synchronized (followers) {
            for (FollowerInfo fi : followers) {
                if (fi.getHost().equals(followerHost) && fi.getPort() == followerPort) {
                    return; // already known
                }
            }
            followers.add(new FollowerInfo(followerHost, followerPort));
            System.out.println("Follower registered: " + followerHost + ":" + followerPort);
        }
    }

    /**
     * Inner record storing follower info.
     */
    private static class FollowerInfo {
        private final String host;
        private final int port;

        public FollowerInfo(String host, int port) {
            this.host = host;
            this.port = port;
        }

        public String getHost() {
            return host;
        }

        public int getPort() {
            return port;
        }
    }
}