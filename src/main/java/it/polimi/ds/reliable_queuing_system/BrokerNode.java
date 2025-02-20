package it.polimi.ds.reliable_queuing_system;

import java.util.List;
import java.util.Map;

/**
 * Defines the basic interface any broker should implement.
 * LeaderBroker, FollowerBroker, etc., all implement these methods.
 */
public interface BrokerNode {

    // Start and stop the broker's server
    void startBroker();
    void stopBroker();

    // Leadership
    boolean isLeader();

    // Queue operations
    void createQueue(String queueName);
    void appendData(String queueName, int data);

    // Read data (follower may forward to leader or handle locally)
    Integer readData(String queueName, String clientId);

    // Snapshot of all queues
    Map<String, List<Integer>> getAllQueueData();

    // Leader -> Follower data replication
    void replicateDataToFollowers(String queueName, int data);

    // For offset replication: Leader -> Follower offset updates
    void replicateOffsetToFollowers(String queueName, String clientId, int newOffset);

    // Fetch a client's offset from local store
    int fetchClientOffset(String queueName, String clientId);

    // Update a client's offset in local store
    void updateClientOffset(String queueName, String clientId, int newOffset);
}