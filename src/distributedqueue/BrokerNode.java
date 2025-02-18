package distributedqueue;

import java.util.List;
import java.util.Map;

/**
 * Represents a broker node in the distributed queue system. A broker can be either a leader or a follower.
 *
 * Requirements addressed in this interface reference:
 * 1 (leader-follower), 2 (heartbeats), 3 (leader election), 5 & 6 (client connections), 7 (next data element),
 * 8 & 9 & 10 (multiple clients, queues, and brokers), 11 (stable storage), 12 (recovery), 13 (client ID),
 * 14 (client read offsets), 15 (reconnection from last offset).
 *
 * Networking aspects (LAN environment):
 *  - BrokerNode implementers should handle remote communication for read/write/replication.
 */
public interface BrokerNode {

    /**
     * Start the broker (including any networking, internal state initialization, heartbeats).
     */
    void startBroker();

    /**
     * Stop the broker (clean shutdown).
     */
    void stopBroker();

    /**
     * Create a new queue on the broker (leader-only).
     * @param queueName The name of the queue to be created.
     */
    void createQueue(String queueName);

    /**
     * Append data (an integer) to an existing queue (leader-only).
     * @param queueName The queue to append data to.
     * @param data The integer data to append.
     */
    void appendData(String queueName, int data);

    /**
     * Read the next data item for a particular client from a queue (leader or follower).
     * @param queueName The name of the queue to read from.
     * @param clientId Unique identifier of the client.
     * @return The next integer data for the client, or null if none available.
     */
    Integer readData(String queueName, String clientId);

    /**
     * Replicate queue data to the follower brokers (leader-only or triggered by the leader).
     * @param queueName The name of the queue for replication.
     * @param newData The new data appended.
     */
    void replicateDataToFollowers(String queueName, int newData);

    /**
     * Synchronize data with the current leader if this broker is a follower and needs to recover.
     */
    void synchronizeWithLeader();

    /**
     * Return true if this broker is currently the leader.
     */
    boolean isLeader();

    /**
     * Persist the client read offset to stable storage.
     * @param queueName The name of the queue.
     * @param clientId The client identifier.
     * @param offset The offset to persist.
     */
    void persistClientOffset(String queueName, String clientId, int offset);

    /**
     * Returns a snapshot of all queue data for replication or recovery usage.
     */
    Map<String, List<Integer>> getAllQueueData();

    /**
     * In a LAN environment, each broker should know how to connect or be reached by other brokers.
     * Provide the local server port to connect or publicly advertise.
     */
    int getBrokerPort();
}