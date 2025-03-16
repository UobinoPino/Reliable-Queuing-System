package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.Message;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

/// A class that contains the state replicated over all the brokers.
public class SharedState {
    private final AtomicInteger nextClientIdAvailable = new AtomicInteger(0);
    private final AtomicInteger nextBrokerIdAvailable = new AtomicInteger(0);

    private final Map<String, List<Integer>> queues = new ConcurrentHashMap<>();
    private final Map<Integer, Map<String, Integer>> clientOffsets = new ConcurrentHashMap<>();

    private final Map<Integer, String> knownBrokers = new ConcurrentHashMap<>();
    private final AtomicInteger leaderId = new AtomicInteger();

    private final List<Message> log = new CopyOnWriteArrayList<>();

    // Track last time we received a heartbeat from the leader
    private volatile long lastHeartbeatFromLeader = System.currentTimeMillis();
    // For the leader, track the time of the last ack from each broker
    private final Map<Integer, Long> lastHeartbeatAckMap = new ConcurrentHashMap<>();



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
    /// Returns -1 if no offset is found.
    public int getClientOffset(int clientId, String queueName) {
        return getClientOffsets(clientId).getOrDefault(queueName, -1);
    }

    /// Returns the number of brokers currently connected.
    public int getBrokersCount() {
        return knownBrokers.size();
    }

    /// Returns the address of the broker matching the given id.
    /// (Addresses are strings in the form "<ip>:<port>").
    public String getBrokerAddress(int brokerId) {
        return knownBrokers.get(brokerId);
    }

    /// Adds a new broker with given id and address into the known brokers map.
    public void addBrokerAddress(int brokerId, String address) {
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

    /// Appends a new message to the broker's log.
    public void addLogEntry(Message message) {
        log.add(message);
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
    }

    // Called by the leader when it receives an ack from a follower
    public void updateLastHeartbeatAckReceived(int followerId) {
        lastHeartbeatAckMap.put(followerId, System.currentTimeMillis());
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
            if (diff > heartbeatTimeoutMs) {
                // Mark broker as removed or handle re-election logic
                System.out.println("Leader: Broker " + brokerId + " timed out, removing...");
                removeBrokerAddress(brokerId);
            }
        }
    }

    // Follower checks if the leader has timed out
    public void checkLeaderTimeout(int myId, long heartbeatTimeoutMs) {
        long now = System.currentTimeMillis();
        long diff = now - lastHeartbeatFromLeader;
        if (diff > heartbeatTimeoutMs) {
            // Follower suspects leader is dead
            System.out.println("Follower " + myId + ": Leader " + getLeaderId() + " has not sent heartbeat. "
                    + "Consider leader failed. (No re-election logic here.)");
        }
    }





}