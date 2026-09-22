package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.utils.Address;
import it.polimi.ds.reliable_queuing_system.utils.AddressJsonAdapter;
import it.polimi.ds.reliable_queuing_system.utils.LogEntry;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import java.io.*;
import java.util.*;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;


/// A class that contains the state replicated over all the brokers.
public class SharedState implements Serializable {
    // region PERSISTENCE-RELATED FIELDS
    private static final String DATA_DIR = "broker_data";
    private static final String QUEUES_FILE = DATA_DIR + "/queues.json";
    private static final String OFFSETS_FILE = DATA_DIR + "/client_offsets.json";
    private static final String CLIENT_ADDRESSES_FILE = DATA_DIR + "/client_addresses.json";

    private transient Gson gson = initGson();
    //endregion

    //region BROKER-RELATED FIELDS
    private final AtomicInteger nextBrokerIdAvailable = new AtomicInteger(0);
    private final Map<Integer, Address> knownBrokers = new ConcurrentHashMap<>();
    private final AtomicInteger leaderId = new AtomicInteger();
    private final AtomicInteger currentEpoch = new AtomicInteger(0);
    //endregion

    //region CLIENT-RELATED FIELDS
    private final AtomicInteger nextClientIdAvailable = new AtomicInteger(0);
    private final Map<String, List<Integer>> queues = new ConcurrentHashMap<>();
    private final Map<Integer, Map<String, Integer>> clientOffsets = new ConcurrentHashMap<>();
    private final Map <Address, Integer> clientAddressMap = new ConcurrentHashMap<>();
    //endregion

    //region LOG-RELATED FIELDS
    private final List<LogEntry> waitingAckEntries = new CopyOnWriteArrayList<>();
    private final List<LogEntry> waitingCommitEntries = new CopyOnWriteArrayList<>();
    private final List<LogEntry> pendingEntries = new CopyOnWriteArrayList<>();
    private final List<LogEntry> log = new CopyOnWriteArrayList<>();

    /// For every entry still waiting for ACKs (key = entry index), the set of ids of the
    /// followers that already acknowledged it. A Set is used so that duplicated ACKs
    /// coming from the same follower are counted only once.
    private final Map<Integer, Set<Integer>> propagationAcks = new ConcurrentHashMap<>();
    //endregion


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


    /// Returns a list of entries waiting for commit
    public List<LogEntry> getWaitingCommitEntries() {
        return new ArrayList<>(waitingCommitEntries);
    }

    /// Return a snapshot of entries waiting for ACK
    public synchronized List<LogEntry> getWaitingAckEntries() {
        return new ArrayList<>(waitingAckEntries);
    }

    /// Clear all entries waiting for ACK
    public synchronized void clearWaitingAckEntries() {
        waitingAckEntries.clear();
        propagationAcks.clear();
    }

    //region PERSISTENCE-RELATED METHODS

    /// Initialize the Gson object with a custom serialization/deserialization adapter for the [Address] type.
    private Gson initGson() {
        return new GsonBuilder()
                .registerTypeAdapter(Address.class, new AddressJsonAdapter())
                .enableComplexMapKeySerialization()
                .setPrettyPrinting()
                .create();
    }

    /// Method called while reconstructing the `SharedState` object after deserialization
    /// (needed since the `gson` field is transient and therefore needs to be recreated after deserialization).
    @Serial
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        gson = initGson();
    }

    /// Load persisted state from disk
    private void loadPersistedState() {
        loadQueues();
        loadClientOffsets();
        loadClientAddresses();
    }

    /// Load queues from disk
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

    /// Load client offsets from disk
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

    /// Load the map between client addresses and client IDs from disk
    private void loadClientAddresses() {
        try {
            File file = new File(CLIENT_ADDRESSES_FILE);
            if (file.exists()) {
                String json = new String(Files.readAllBytes(file.toPath()));
                Type type = new TypeToken<Map<Address, Integer>>(){}.getType();
                Map<Address, Integer> loadedAddresses = gson.fromJson(json, type);

                if (loadedAddresses != null) {
                    clientAddressMap.putAll(loadedAddresses);
                    nextClientIdAvailable.set(clientAddressMap.size());
                    System.out.println("Loaded " + loadedAddresses.size() + " client addresses from disk");
                }
            }
        } catch (IOException e) {
            System.err.println("Failed to load client addresses: " + e.getMessage());
        }
    }

    /// Persist queues to disk
    private void persistQueues() {
        try {
            String json = gson.toJson(queues);
            Files.write(Paths.get(QUEUES_FILE), json.getBytes());
        } catch (IOException e) {
            System.err.println("Failed to persist queues: " + e.getMessage());
        }
    }

    /// Persist client offsets to disk
    private void persistClientOffsets() {
        try {
            String json = gson.toJson(clientOffsets);
            Files.write(Paths.get(OFFSETS_FILE), json.getBytes());
        } catch (IOException e) {
            System.err.println("Failed to persist client offsets: " + e.getMessage());
        }
    }

    /// Persist the map between client addresses and client IDs to disk
    private void persistClientAddresses() {
        try {
            String json = gson.toJson(clientAddressMap);
            Files.write(Paths.get(CLIENT_ADDRESSES_FILE), json.getBytes());
        } catch (IOException e) {
            System.err.println("Failed to persist client addresses: " + e.getMessage());
        }
    }

    /// Persist current state to disk
    public void persistState() {
        // Create data directory if it doesn't exist
        try {
            Files.createDirectories(Paths.get(DATA_DIR));
        } catch (IOException e) {
            System.err.println("Failed to create data directory: " + e.getMessage());
        }

        persistQueues();
        persistClientOffsets();
        persistClientAddresses();
    }

    //endregion


    //region BROKER-TO-BROKER METHODS

    /// Returns the next broker id available for the system
    /// (and increments the counter accordingly).
    public int getNewBrokerId() {
        return nextBrokerIdAvailable.getAndIncrement();
    }

    /// Returns the number of brokers currently connected.
    public int getBrokersCount() {
        return knownBrokers.size();
    }

    /// Returns the address of the broker matching the given id.
    public Address getBrokerAddress(int brokerId) {
        return knownBrokers.get(brokerId);
    }

    /// Returns a copy of the map containing broker addresses and IDs.
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

    /// Sets the id of the current leader broker to the given value, and increment the current epoch.
    public synchronized void setNewLeaderId(int leaderId) {
        this.leaderId.set(leaderId);
        this.currentEpoch.incrementAndGet();
    }

    //endregion


    //region CLIENT-TO-BROKER METHODS

    /// Returns the ID corresponding to the given client address
    /// (or a new one if it's the first connection from a client with this address)
    public synchronized int getClientId(Address clientAddress) {
        Integer clientId = clientAddressMap.get(clientAddress);
        if (clientId == null) {
            clientId = nextClientIdAvailable.getAndIncrement();
            clientAddressMap.put(clientAddress, clientId);
            persistClientAddresses();
            System.out.println("[INFO]: New client registered with id " + clientId + " and address " + clientAddress);
        }
        else {
            System.out.println("[INFO]: Client " + clientId + " with address " + clientAddress + " reconnected");
        }
        return clientId;
    }

    /// Returns the queue identified by queueName. If it doesn't exist, a new one is created.
    public List<Integer> getQueue(String queueName) {
        return queues.computeIfAbsent(queueName, key -> new CopyOnWriteArrayList<>());
    }

    /// Adds an item to the queue identified by queueName.
    public synchronized void addToQueue(String queueName, int item) {
        getQueue(queueName).add(item);
        persistQueues();
    }

    /// Returns the offsets for a given client id.
    /// If no offsets exist yet, an empty map is created.
    public Map<String, Integer> getClientOffsets(int clientId) {
        return clientOffsets.computeIfAbsent(clientId, key -> new ConcurrentHashMap<>());
    }

    /// Updates the offset for a specific client and queue.
    public synchronized void updateClientOffset(int clientId, String queueName, int newOffset) {
        getClientOffsets(clientId).put(queueName, newOffset);
        persistClientOffsets();
    }

    /// Retrieves the offset for a specific client and queue.
    /// Returns 0 if no offset is found (so that the clients starts to read from the beginning).
    public int getClientOffset(int clientId, String queueName) {
        return getClientOffsets(clientId).getOrDefault(queueName, 0);
    }

    /// Returns the current system epoch.
    public int getCurrentEpoch() {
        return currentEpoch.get();
    }

    //endregion


    //region LOG-RELATED METHODS

    /// Adds a new [LogEntry] to the list of entries waiting for an ack.
    public void addWaitingAckEntry(LogEntry logEntry) {
        waitingAckEntries.add(logEntry);
    }

    /// Adds a new [LogEntry] to the list of entries waiting for the commit.
    public void addWaitingCommitEntry(LogEntry logEntry) {
        waitingCommitEntries.add(logEntry);
    }

    /// Returns whether the given [LogEntry] is waiting for Acks or not.
    public boolean isEntryWaitingAck(LogEntry logEntry) {
        return waitingAckEntries.contains(logEntry);
    }

    /// Returns the number of *follower* ACKs the leader needs before committing an entry.
    ///
    /// An entry can be committed only when it is stored by a **strict majority** of the
    /// brokers, i.e. by `floor(n/2) + 1` of them. The leader already stores the entry
    /// itself, so its own copy is the `+1`: it only has to collect `floor(n/2)` ACKs.
    ///
    /// Examples: n=2 -> 1 ACK, n=3 -> 1 ACK, n=4 -> 2 ACKs, n=5 -> 2 ACKs, n=7 -> 3 ACKs.
    ///
    /// Requiring a majority here (instead of a single ACK) is what makes the protocol safe:
    /// since a new leader is also elected by a majority, the two sets necessarily overlap on
    /// at least one broker, and since the candidate with the longest log wins the election,
    /// the new leader is guaranteed to own every already-committed entry.
    public int getRequiredAcks() {
        return knownBrokers.size() / 2;
    }

    /// Registers the ACK sent by broker `senderId` for the given [LogEntry] and returns
    /// whether the entry has to be committed now.
    ///
    /// It returns `true` for **exactly one** ACK per entry: the one that brings the entry
    /// to the majority quorum. Late/duplicated ACKs (and ACKs for entries this broker is no
    /// longer waiting for) return `false`, so the entry can never be committed twice.
    public synchronized boolean registerAckAndCheckQuorum(LogEntry entry, int senderId) {
        // ignore ACKs for entries that are not waiting for an ACK anymore
        // (already committed, or never propagated by this broker)
        if (!waitingAckEntries.contains(entry)) {
            return false;
        }

        // register the ACK (duplicated ACKs from the same broker are ignored by the Set)
        Set<Integer> acks = propagationAcks.computeIfAbsent(entry.index(), k -> new HashSet<>());
        acks.add(senderId);

        int requiredAcks = getRequiredAcks();
        System.out.println("[INFO]: Entry " + entry.index() + " ACK count: " + acks.size() + "/" + requiredAcks
                + " (cluster size: " + knownBrokers.size() + ")");

        // not enough ACKs yet: keep waiting
        if (acks.size() < requiredAcks) {
            return false;
        }

        // quorum reached: "claim" the entry by moving it out of the waiting-ACK list, so that
        // no concurrent ACK for the same entry can reach this point and commit it a second time
        waitingAckEntries.remove(entry);
        waitingCommitEntries.add(entry);
        return true;
    }

    /// Re-evaluates the quorum of an entry that is still waiting for ACKs, without registering
    /// a new one. It is used by the leader after removing crashed brokers: a smaller cluster
    /// means a smaller majority, so entries that were blocked may have become committable.
    ///
    /// As [#registerAckAndCheckQuorum], it returns `true` at most once per entry.
    public synchronized boolean recheckQuorum(LogEntry entry) {
        if (!waitingAckEntries.contains(entry)) {
            return false;
        }

        int receivedAcks = propagationAcks.getOrDefault(entry.index(), Collections.emptySet()).size();
        if (receivedAcks < getRequiredAcks()) {
            return false;
        }

        waitingAckEntries.remove(entry);
        waitingCommitEntries.add(entry);
        return true;
    }

    /// Tries to permanently add the given [LogEntry] to the log,
    /// and returns if the operation has been successful or not.
    public synchronized boolean commitEntry(LogEntry logEntry) {
        // the ACKs collected for this entry are not needed anymore
        propagationAcks.remove(logEntry.index());

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
            System.out.println("[INFO]: Trying to commit an entry that is already in the log. Ignoring...");
            return false;  // entry already committed, no need to do anything else
        } else {
            System.out.println("[INFO]: Trying to commit an entry that is not in any waiting list. Accepting it anyway...");

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


        return true;
    }

    /// Returns the current length of the broker's log.
    public int getLogLength() {
        return log.size();
    }

    /// Replace the current log with the given one.
    public synchronized void replaceLog(List<LogEntry> newLog) {
        log.clear();
        log.addAll(newLog);
        System.out.println("[INFO]: Log replaced with leader's log containing " + newLog.size() + " entries");
    }

    /// Return a copy of the current log.
    public List<LogEntry> getCompleteLog() {
        return new ArrayList<>(log);
    }

    //endregion
}