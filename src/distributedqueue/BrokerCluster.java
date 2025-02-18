package distributedqueue;

import java.util.ArrayList;
import java.util.List;

/**
 * Manages a cluster of brokers in a very simplified manner:
 *  - We can manually add brokers and designate which one is leader.
 *  - We can request failover (manual approach).
 *
 * For real systems, you'd have a robust distributed consensus or election algorithm (e.g., Raft/ZooKeeper).
 *
 * Requirements addressed:
 * 1 (leader-follower), 2 (heartbeats placeholder), 3 (leader election placeholder),
 * 10 (multiple brokers), 12 (recovery), 15 (clients can reconnect).
 */
public class BrokerCluster {
    private BrokerNode leader;
    private final List<BrokerNode> brokers;

    public BrokerCluster() {
        this.brokers = new ArrayList<>();
    }

    /**
     * Add a broker to the cluster. If it's a LeaderBroker, we can set it as leader if not set.
     */
    public void addBroker(BrokerNode broker) {
        this.brokers.add(broker);
        if (leader == null && broker.isLeader()) {
            leader = broker;
        }
    }

    /**
     * Start all brokers in the cluster.
     * Implement heartbeat checks for real systems.
     */
    public void startAllBrokers() {
        for (BrokerNode broker : brokers) {
            broker.startBroker();
        }
    }

    /**
     * Stop all brokers.
     */
    public void stopAllBrokers() {
        for (BrokerNode broker : brokers) {
            broker.stopBroker();
        }
    }

    /**
     * Trigger leader failover in a naive way.
     * In production, you'd have a robust approach for detecting the current leader's failure.
     */
    public void triggerLeaderFailover() {
        System.out.println("Leader failover triggered. Old leader: " + (leader != null ? leader.toString() : "none"));
        // Just pick another broker to be the new leader (requires manual reconfiguration in real usage).
        for (BrokerNode broker : brokers) {
            if (!broker.isLeader()) {
                // In a real system, you'd have a better approach to re-initialize states.
                // We'll just set this as "leader" notionally.
                leader = broker;
                System.out.println("New leader: " + broker);
                break;
            }
        }
    }

    public BrokerNode getLeader() {
        return leader;
    }
}