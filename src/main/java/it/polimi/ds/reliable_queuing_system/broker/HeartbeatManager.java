package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.BrokerRemoval;
import it.polimi.ds.reliable_queuing_system.messages.Heartbeat;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class HeartbeatManager {
    public HeartbeatManager(int brokerId, SharedState sharedState, AtomicBoolean electionInProgress) {
        this.brokerId = brokerId;
        this.sharedState = sharedState;
        this.electionInProgress = electionInProgress;
        this.heartbeatThreadExecutor = Executors.newSingleThreadScheduledExecutor();

        this.lastHeartbeats = new ConcurrentHashMap<>();
        this.missedHeartbeats = new ConcurrentHashMap<>();

        if (sharedState.getLeaderId() == brokerId) {
            heartbeatThreadExecutor.scheduleAtFixedRate(this::heartbeatSenderTask, 0, HEARTBEAT_INTERVAL_MS, TimeUnit.MILLISECONDS);
        } else {
            // start the monitor task with a random delay to minimize simultaneous election triggering
            Random random = new Random();
            int randomDelayMs = random.nextInt(200);

            heartbeatThreadExecutor.scheduleAtFixedRate(
                this::heartbeatMonitorTask,
                HEARTBEAT_INTERVAL_MS + randomDelayMs,
                HEARTBEAT_INTERVAL_MS,
                TimeUnit.MILLISECONDS
            );
        }
    }

    public static final long HEARTBEAT_INTERVAL_MS = 500;  //TODO: maybe increase?
    public static final long HEARTBEAT_TIMEOUT_MS = 2000;  //TODO: maybe increase?
    public static final int MISSABLE_HEARTBEATS = 2;

    private final int brokerId;
    private final SharedState sharedState;
    private final AtomicBoolean electionInProgress;
    private final ScheduledExecutorService heartbeatThreadExecutor;

    private final Map<Integer, Long> lastHeartbeats;
    private final Map<Integer, Integer> missedHeartbeats;


    /// Updates the last time an `HeartBeat` message has been received from the leader.
    public void updateLastHeartbeatReceived() {
        int leaderId = sharedState.getLeaderId();
        lastHeartbeats.put(leaderId, System.currentTimeMillis());
        missedHeartbeats.put(leaderId, 0);
    }

    /// Updates the last time an `HeartbeatAck` message has been received from the given follower.
    public void updateLastHeartbeatAckReceived(int brokerId) {
        lastHeartbeats.put(brokerId, System.currentTimeMillis());
        missedHeartbeats.put(brokerId, 0);
    }

    /// Task periodically executed by the leader to send an `Heartbeat` message to all followers
    /// and check if any of them has crashed (and eventually broadcast their removal to remaining followers).
    private void heartbeatSenderTask() {
        // send heartbeat to all active followers
        NetworkManager.broadcastMessage(new Heartbeat(), brokerId, sharedState);

        // Check for follower timeouts
        Set<Integer> removedBrokers = checkFollowersTimeouts();

        // Process broker removals
        for (Integer broker : removedBrokers) {
            //TODO: is it right to broadcast the broker removal here?
            // Should it be treated as an entry propagation instead?
            System.out.println("[INFO]: Broker " + broker + " failed. Asking other followers to remove it...");
            NetworkManager.broadcastMessage(new BrokerRemoval(broker), brokerId, sharedState);
        }
    }

    /// Task periodically executed by the followers to check if the leader has crashed
    /// (and eventually start a new election).
    private void heartbeatMonitorTask() {
        if (!electionInProgress.get()) {
            // Check if the leader has timed out
            boolean leaderFailed = checkLeaderTimeout();

            if (leaderFailed) {
                // if still no election is in progress, start the election
                if (electionInProgress.compareAndSet(false, true)) {
                    //TODO: start leader election
                }
                else {
                    System.out.println("[INFO]: Leader failure detected but someone has already started an election in the meantime.");
                }
            }
        }
        else {
            System.out.println("[INFO]: Skipping heartbeat monitor task because an election is already in progress.");
        }
    }

    /// Returns the IDs of the followers that have timed out.`
    private Set<Integer> checkFollowersTimeouts() {
        long now = System.currentTimeMillis();
        Set<Integer> knownBrokers = sharedState.getBrokerAddresses().keySet();
        Set<Integer> removedBrokers = new HashSet<>();

        for (Integer bId : knownBrokers) {
            if (bId == brokerId) {
                continue; // skip self
            }

            Long lastAckTime = lastHeartbeats.getOrDefault(bId, 0L);
            long diff = now - lastAckTime;
            if (diff > HEARTBEAT_TIMEOUT_MS) {
                // Increment missed heartbeats counter
                int missed = missedHeartbeats.getOrDefault(bId, 0) + 1;
                System.out.println("[INFO]: Broker " + bId + " missed " + missed + " heartbeats");

                if (missed > MISSABLE_HEARTBEATS) {
                    System.out.println("[INFO]: Removing broker " + bId + "...");
                    removedBrokers.add(bId);
                    sharedState.removeBrokerAddress(bId);
                    missedHeartbeats.remove(brokerId);
                }
                else {
                    System.out.println("[INFO]: not removing yet.");
                    missedHeartbeats.put(brokerId, missed);
                }
            }
        }

        return removedBrokers;
    }

    /// Returns whether the leader has timed out or not.
    private boolean checkLeaderTimeout() {
        long now = System.currentTimeMillis();
        int leaderId = sharedState.getLeaderId();

        //TODO: worth keeping even if it should never be executed?
        if (leaderId == brokerId) {
            // I'm the leader, can't time out myself
            System.out.println("NON DOVREBBE MAI ENTRARE QUI!");
            return false;
        }

        long diff = now - lastHeartbeats.getOrDefault(leaderId, 0L);
        if (diff > HEARTBEAT_TIMEOUT_MS) {
            // Increment missed heartbeats counter
            int missed = missedHeartbeats.getOrDefault(leaderId, 0) + 1;
            System.out.println("[INFO]: Leader " + leaderId + " missed " + missed + " heartbeats");

            // Only consider leader failed after multiple missed heartbeats
            if (missed > MISSABLE_HEARTBEATS) {
                // Follower suspects leader is dead
                System.out.println("Considering leader failed. (Starting leader election)");
                missedHeartbeats.remove(leaderId);
                sharedState.removeBrokerAddress(leaderId);
                return true; // Trigger leader election
            } else {
                System.out.println("[INFO]: not considering failed yet.");
                missedHeartbeats.put(leaderId, missed);
                return false;
            }
        }

        return false;
    }
}
