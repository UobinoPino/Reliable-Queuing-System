package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.utils.Address;
import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;


import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.*;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
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

    // File paths for persistence
    private static final String DATA_DIR = "broker_data";
    private static final String QUEUES_FILE = DATA_DIR + "/queues.json";
    private static final String OFFSETS_FILE = DATA_DIR + "/client_offsets.json";

    // Gson instance for JSON serialization/deserialization
    private transient Gson gson = new GsonBuilder().setPrettyPrinting().create();


    public SharedState() {
        // Create data directory if it doesn't exist
        try {
            Files.createDirectories(Paths.get(DATA_DIR));
        } catch (IOException e) {
            System.err.println("Failed to create data directory: " + e.getMessage());
        }

        // Load persisted data if available
        loadPersistedState();
    }
    /**
     * Load persisted state from disk
     */
    private void loadPersistedState() {
        loadQueues();
        loadClientOffsets();
    }
    /**
    * Load queues from disk
     */
    private void loadQueues() {
        try {
            File file = new File(QUEUES_FILE);
            if (file.exists()) {
                String json = new String(Files.readAllBytes(file.toPath()));
                Type type = new TypeToken<Map<String, List<Integer>>>(){}.getType();
                Map<String, List<Integer>> loadedQueues = gson.fromJson(json, type);

                if (loadedQueues != null) {
                    // Convert to CopyOnWriteArrayList for thread safety
                    loadedQueues.forEach((queueName, items) -> {
                        CopyOnWriteArrayList<Integer> threadSafeList = new CopyOnWriteArrayList<>(items);
                        queues.put(queueName, threadSafeList);
                    });
                    System.out.println("Loaded " + loadedQueues.size() + " queues from disk");
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to load queues: " + e.getMessage());
        }
    }

    /**
     * Load client offsets from disk
     */
    private void loadClientOffsets() {
        try {
            File file = new File(OFFSETS_FILE);
            if (file.exists()) {
                String json = new String(Files.readAllBytes(file.toPath()));
                Type type = new TypeToken<Map<Integer, Map<String, Integer>>>(){}.getType();
                Map<Integer, Map<String, Integer>> loadedOffsets = gson.fromJson(json, type);

                if (loadedOffsets != null) {
                    // Convert to ConcurrentHashMap for thread safety
                    loadedOffsets.forEach((clientId, offsets) -> {
                        ConcurrentHashMap<String, Integer> threadSafeMap = new ConcurrentHashMap<>(offsets);
                        clientOffsets.put(clientId, threadSafeMap);
                    });
                    System.out.println("Loaded offsets for " + loadedOffsets.size() + " clients from disk");
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to load client offsets: " + e.getMessage());
        }
    }

    /**
     * Persist current state to disk
     */
    public void persistState() {
        persistQueues();
        persistClientOffsets();
    }

    /**
     * Persist queues to disk
     */
    private void persistQueues() {
        try {
            String json = gson.toJson(queues);
            Files.write(Paths.get(QUEUES_FILE), json.getBytes());
        } catch (IOException e) {
            System.err.println("Failed to persist queues: " + e.getMessage());
        }
    }

    /**
     * Persist client offsets to disk
     */
    private void persistClientOffsets() {
        try {
            String json = gson.toJson(clientOffsets);
            Files.write(Paths.get(OFFSETS_FILE), json.getBytes());
        } catch (IOException e) {
            System.err.println("Failed to persist client offsets: " + e.getMessage());
        }
    }
    private void readObject(java.io.ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        // Recreate the Gson instance after deserialization
        gson = new GsonBuilder().setPrettyPrinting().create();
    }



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
        persistQueues();
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
        persistClientOffsets();
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
}