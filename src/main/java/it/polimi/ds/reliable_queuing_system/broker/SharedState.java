package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.utils.Address;
import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/// A class that contains the state replicated over all the brokers.
public class SharedState implements Serializable {
    private final AtomicInteger nextClientIdAvailable = new AtomicInteger(0);
    private final AtomicInteger nextBrokerIdAvailable = new AtomicInteger(0);

    private final Map<String, List<Integer>> queues = new ConcurrentHashMap<>();
    private final Map<Integer, Map<String, Integer>> clientOffsets = new ConcurrentHashMap<>();

    private final Map<Integer, Address> knownBrokers = new ConcurrentHashMap<>();
    private final AtomicInteger leaderId = new AtomicInteger();

    private final List<LogEntry> waitingAckEntries = new CopyOnWriteArrayList<>();
    private final List<LogEntry> waitingCommitEntries = new CopyOnWriteArrayList<>();
    private final List<LogEntry> pendingEntries = new CopyOnWriteArrayList<>();
    private final List<LogEntry> log = new CopyOnWriteArrayList<>();

    // Track last time we received a heartbeat from the leader
    private volatile long lastHeartbeatFromLeader = System.currentTimeMillis();
    // For the leader, track the time of the last ack from each broker
    private final Map<Integer, Long> lastHeartbeatAckMap = new ConcurrentHashMap<>();
    // Add this field to the SharedState class
    private final Map<Integer, Integer> missedHeartbeats = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> missedLeaderHeartbeats = new ConcurrentHashMap<>();



    /// Returns the next client id available for the system
    /// (and increments the counter accordingly).
    public int getNewClientId() {
        return nextClientIdAvailable.getAndIncrement();
    }

    /// Returns the next broker id available for the system
    /// (and increments the counter accordingly).
    public int getNewBrokerId() {
        return nextBrokerIdAvailable.getAndIncrement();
    }

    // Methods to access and manipulate queues

    /// Returns the queue identified by queueName. If it doesn't exist, a new one is created.
    public List<Integer> getQueue(String queueName) {
        return queues.computeIfAbsent(queueName, key -> new CopyOnWriteArrayList<>());
    }

    /// Adds an item to the queue identified by queueName.
    public void addToQueue(String queueName, int item) {
        getQueue(queueName).add(item);
    }


    // Methods to access and manipulate client offsets

    /// Returns the offsets for a given client id.
    /// If no offsets exist yet, an empty map is created.
    public Map<String, Integer> getClientOffsets(int clientId) {
        return clientOffsets.computeIfAbsent(clientId, key -> new ConcurrentHashMap<>());
    }

    /// Updates the offset for a specific client and queue.
    public void updateClientOffset(int clientId, String queueName, int newOffset) {
        getClientOffsets(clientId).put(queueName, newOffset);
    }

    /// Retrieves the offset for a specific client and queue.
    /// Returns 0 if no offset is found (so that the clients starts to read from the beginning).
    public int getClientOffset(int clientId, String queueName) {
        return getClientOffsets(clientId).getOrDefault(queueName, 0);
    }

    /// Returns the number of brokers currently connected.
    public int getBrokersCount() {
        return knownBrokers.size();
    }

    /// Returns the address of the broker matching the given id.
    public Address getBrokerAddress(int brokerId) {
        return knownBrokers.get(brokerId);
    }

    public Map<Integer, Address> getBrokerAddresses() {
        return new HashMap<>(knownBrokers);
    }

    /// Adds a new broker with given id and address into the known brokers map.
    public void addBrokerAddress(int brokerId, Address address) {
        knownBrokers.put(brokerId, address);
    }

    /// Removes a broker and its address from the known brokers map.
    public void removeBrokerAddress(int brokerId) {
        knownBrokers.remove(brokerId);
    }

    /// Returns the id of the current leader broker.
    public Integer getLeaderId() {
        return leaderId.get();
    }

    /// Sets the id of the current leader broker to the given value.
    public void setNewLeaderId(int leaderId) {
        this.leaderId.set(leaderId);

        lastHeartbeatFromLeader = System.currentTimeMillis();
    }

    /// Returns whether the broker with the given id is the leader.
    public boolean isLeader(int brokerId) {
        return leaderId.get() == brokerId;
    }

    /// Adds a new [LogEntry] to the list of entries waiting for an ack.
    public void addWaitingAckEntry(LogEntry logEntry) {
        waitingAckEntries.add(logEntry);
    }

    /// Adds a new [LogEntry] to the list of entries waiting for the commit.
    public void addWaitingCommitEntry(LogEntry logEntry) {
        waitingCommitEntries.add(logEntry);
    }

    public boolean isEntryWaitingAck(LogEntry logEntry) {
        return waitingAckEntries.contains(logEntry);
    }


    public void commitEntry(LogEntry logEntry) {
        //TODO: is it ok to call indexOf directly on the object?
        // Or are they different objects if they have been passed through the network?

        // remove the given entry from the waiting list it's currently in (if any)
        int waitingAckPos = waitingAckEntries.indexOf(logEntry);
        int waitingCommitPos = waitingCommitEntries.indexOf(logEntry);
        int pendingPos = pendingEntries.indexOf(logEntry);
        if (waitingAckPos >= 0) {
            waitingAckEntries.remove(logEntry);
        } else if (waitingCommitPos >= 0) {
            waitingCommitEntries.remove(logEntry);
        } else if (pendingPos >= 0) {
            pendingEntries.remove(logEntry);
        } else if (log.contains(logEntry)) {
            return;  // entry already committed, no need to do anything else
        }

        // if the given entry can be directly added to log...
        if (logEntry.index() == log.size()) {
            // add it
            log.add(logEntry);

            // and also add all the other pending entries that were waiting for it
            pendingEntries.sort(Comparator.comparingInt(LogEntry::index));
            for (LogEntry entry : pendingEntries) {
                if (entry.index() == log.size()) {
                    pendingEntries.remove(entry);
                    log.add(entry);
                }
            }
        }
        // else...
        else {
            // add it to the pending list
            pendingEntries.add(logEntry);
        }


        //FIXME: what happens if there are multiple entries with the same index?
    }

    /// Returns the current length of the broker's log.
    public int getLogLength() {
        return log.size();
    }

    /// Updates this shared state using data from another SharedState instance.
    /// Copies over queue contents, client offsets, known brokers, leaderId, and log entries.
    public void updateFrom(SharedState other) {
        // Update next available IDs
        this.nextClientIdAvailable.set(Math.max(this.nextClientIdAvailable.get(), other.nextClientIdAvailable.get()));
        this.nextBrokerIdAvailable.set(Math.max(this.nextBrokerIdAvailable.get(), other.nextBrokerIdAvailable.get()));

        // Update queues
        other.queues.forEach((key, list) -> {
            this.queues.putIfAbsent(key, new CopyOnWriteArrayList<>());
            this.queues.get(key).addAll(list);
        });

        // Update client offsets
        other.clientOffsets.forEach((clientId, offsets) -> {
            this.clientOffsets.putIfAbsent(clientId, new ConcurrentHashMap<>());
            this.clientOffsets.get(clientId).putAll(offsets);
        });

        // Update known brokers
        this.knownBrokers.putAll(other.knownBrokers);

        // Update leader information
        this.leaderId.set(other.leaderId.get());

        // Update log entries
        this.log.addAll(other.log);

        this.lastHeartbeatFromLeader = System.currentTimeMillis();
    }

    // Called by follower when it receives a heartbeat from the leader
    public void updateLastHeartbeatReceived(int followerId) {
        lastHeartbeatFromLeader = System.currentTimeMillis();
        // Reset missed heartbeats counter when we receive a heartbeat
        missedLeaderHeartbeats.put(followerId, 0);
    }

    // Called by the leader when it receives an ack from a follower
    public void updateLastHeartbeatAckReceived(int followerId) {
        lastHeartbeatAckMap.put(followerId, System.currentTimeMillis());
        // Reset missed heartbeats counter when we receive an ack
        missedHeartbeats.put(followerId, 0);
    }

    // Leader checks if followers have timed out
    public void checkFollowerTimeouts(long heartbeatTimeoutMs) {
        long now = System.currentTimeMillis();
        for (Integer brokerId : knownBrokers.keySet()) {
            if (brokerId.equals(this.getLeaderId())) {
                continue; // skip self
            }
            Long lastAckTime = lastHeartbeatAckMap.get(brokerId);
            if (lastAckTime == null) {
                // If we never got an ack, treat as potential failure if enough time has passed
                lastAckTime = 0L;
            }
            long diff = now - lastAckTime;
            if (diff > 5*heartbeatTimeoutMs) {
                // Increment missed heartbeats counter
                int missed = missedHeartbeats.getOrDefault(brokerId, 0) + 1;
                missedHeartbeats.put(brokerId, missed);
                // Mark broker as removed or handle re-election logic
                // Only remove after missing multiple heartbeats (at least 2)
                if (missed >= 3) {
                    System.out.println("Leader: Broker " + brokerId + " missed " + missed +
                            " heartbeats, removing...");
                    removeBrokerAddress(brokerId);
                    missedHeartbeats.remove(brokerId);
                } else {
                    System.out.println("Leader: Broker " + brokerId + " missed heartbeat " +
                            missed + " of 3 required before removal");
                }
            }
        }
    }

    // Follower checks if the leader has timed out
    public void checkLeaderTimeout(int myId, long heartbeatTimeoutMs) {
        long now = System.currentTimeMillis();
        long diff = now - lastHeartbeatFromLeader;
        if (diff > 5*heartbeatTimeoutMs) {
            // Increment missed heartbeats counter
            int missed = missedLeaderHeartbeats.getOrDefault(myId, 0) + 1;
            missedLeaderHeartbeats.put(myId, missed);

            // Only consider leader failed after multiple missed heartbeats
            if (missed >= 3) {
                // Follower suspects leader is dead
                System.out.println("Follower " + myId + ": Leader " + getLeaderId() +
                        " has missed " + missed + " heartbeats. Consider leader failed. (No re-election logic here.)");
                // Would trigger leader election here if implemented
            } else {
                System.out.println("Follower " + myId + ": Leader " + getLeaderId() +
                        " missed heartbeat " + missed + " of 3 required before considered failed");
            }
        }
    }

}