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

    /// Appends a new message to the broker's log.
    public void addLogEntry(Message message) {
        log.add(message);
    }

    /// Returns the current length of the broker's log.
    public int getLogLength() {
        return log.size();
    }
}
