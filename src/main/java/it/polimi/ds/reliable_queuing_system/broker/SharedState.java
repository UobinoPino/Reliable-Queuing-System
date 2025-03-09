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

    /// Returns the next client id available for the system
    /// (and increment the counter in the SharedState accordingly).
    public int getNewClientId() {
        return nextClientIdAvailable.getAndIncrement();
    }

    /// Returns the next client id available for the system
    /// (and increment the counter in the SharedState accordingly).
    public int getNewBrokerId() {
        return nextBrokerIdAvailable.getAndIncrement();
    }

    //TODO: add methods to access queues and client offsets

    /// Returns the address of the broker matching the given id.
    /// (Addresses are strings in the form `"<ip>:<port>"`).
    public String getBrokerAddress(int brokerId) {
        return knownBrokers.get(brokerId);
    }

    /// Adds a new broker with given id and address to the map of known brokers.
    /// (Addresses are strings in the form `"<ip>:<port>"`).
    public void addBrokerAddress(int brokerId, String address) {
        knownBrokers.put(brokerId, address);
    }

    /// Remove a broker and its address from the map of known brokers.
    public void removeBrokerAddress(int brokerId) {
        knownBrokers.remove(brokerId);
    }

    /// Returns the id of the current leader broker.
    public Integer getLeaderId() {
        return leaderId.get();
    }

    /// Sets the id of the current leader broker to the given one.
    public void setNewLeaderId(int leaderId) {
        this.leaderId.set(leaderId);
    }

    /// Returns whether the broker with the given id is the leader or not.
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
}
