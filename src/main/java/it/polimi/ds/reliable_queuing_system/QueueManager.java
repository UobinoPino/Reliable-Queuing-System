package it.polimi.ds.reliable_queuing_system;

import java.util.*;

/**
 * Manages the in-memory and on-disk state of each queue and the read offsets for each client.
 * For demonstration, everything is quite simple.
 */
public class QueueManager {

    // queueData: Map<queueName, List<Integer>>
    private final Map<String, List<Integer>> queueData = Collections.synchronizedMap(new HashMap<>());

    // clientOffsets: Map<queueName, Map<clientId, int>>
    private final Map<String, Map<String, Integer>> clientOffsets = Collections.synchronizedMap(new HashMap<>());

    public QueueManager() {
        // For demonstration, you may optionally load existing data from disk
        // or keep it purely in-memory.
    }

    public synchronized void createQueue(String queueName) {
        queueData.putIfAbsent(queueName, new ArrayList<>());
        clientOffsets.putIfAbsent(queueName, new HashMap<>());
    }

    public synchronized void appendData(String queueName, int data) {
        queueData.computeIfAbsent(queueName, k -> new ArrayList<>()).add(data);
    }

    /**
     * Reads the next entry in the queue for the client, increments the offset.
     * If no data is available, returns null.
     */
    public synchronized Integer readNext(String queueName, String clientId) {
        List<Integer> list = queueData.get(queueName);
        if (list == null) {
            return null;
        }
        Map<String, Integer> offsetsForQueue = clientOffsets.get(queueName);
        if (offsetsForQueue == null) {
            return null;
        }
        int currentOffset = offsetsForQueue.getOrDefault(clientId, -1);
        int nextIndex = currentOffset + 1;
        if (nextIndex >= list.size()) {
            // No new data
            return null;
        }
        Integer item = list.get(nextIndex);
        // Update offset
        offsetsForQueue.put(clientId, nextIndex);
        return item;
    }

    public synchronized Map<String, List<Integer>> getAllQueuesSnapshot() {
        // Return a copy of queue data
        Map<String, List<Integer>> copy = new HashMap<>();
        for (var entry : queueData.entrySet()) {
            copy.put(entry.getKey(), new ArrayList<>(entry.getValue()));
        }
        return copy;
    }

    public synchronized int getClientOffset(String queueName, String clientId) {
        Map<String, Integer> offsetsForQueue = clientOffsets.get(queueName);
        if (offsetsForQueue == null) {
            return -1;
        }
        return offsetsForQueue.getOrDefault(clientId, -1);
    }

    public synchronized void setClientOffset(String queueName, String clientId, int newOffset) {
        if (!clientOffsets.containsKey(queueName)) {
            clientOffsets.put(queueName, new HashMap<>());
        }
        clientOffsets.get(queueName).put(clientId, newOffset);
    }
}