package it.polimi.ds.reliable_queuing_system;

import java.util.*;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.io.*;
import java.lang.reflect.Type;
import java.util.*;
/**
 * Manages the state of each queue (both in memory and persisted to disk)
 * and the read offsets for each client, ensuring data survives restarts.
 */
public class QueueManager {

    // queueData: Map<queueName, List<Integer>>
    private final Map<String, List<Integer>> queueData = Collections.synchronizedMap(new HashMap<>());

    // clientOffsets: Map<queueName, Map<clientId, int>>
    private final Map<String, Map<String, Integer>> clientOffsets = Collections.synchronizedMap(new HashMap<>());

    private static final String QUEUE_DATA_FILE = "queue_data.json";
    private static final String CLIENT_OFFSETS_FILE = "client_offsets.json";
    private final Gson gson = new Gson();
    public QueueManager() {
        loadQueueDataFromDisk();
        loadClientOffsetsFromDisk();
    }

    public synchronized void createQueue(String queueName) {
        queueData.putIfAbsent(queueName, new ArrayList<>());
        clientOffsets.putIfAbsent(queueName, new HashMap<>());
        saveAll();
    }

    public synchronized void appendData(String queueName, int data) {
        queueData.computeIfAbsent(queueName, k -> new ArrayList<>()).add(data);
        saveAll();
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
        saveAll();
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
            saveAll();
        }
        clientOffsets.get(queueName).put(clientId, newOffset);
        saveAll();
    }
    private void loadQueueDataFromDisk() {
        File file = new File(QUEUE_DATA_FILE);
        if (file.exists()) {
            try (Reader reader = new FileReader(file)) {
                Type typeOfMap = new TypeToken<Map<String, List<Integer>>>() {}.getType();
                Map<String, List<Integer>> loaded = gson.fromJson(reader, typeOfMap);
                if (loaded != null) {
                    queueData.clear();
                    queueData.putAll(loaded);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    private void loadClientOffsetsFromDisk() {
        File file = new File(CLIENT_OFFSETS_FILE);
        if (file.exists()) {
            try (Reader reader = new FileReader(file)) {
                Type typeOfMap = new TypeToken<Map<String, Map<String, Integer>>>() {}.getType();
                Map<String, Map<String, Integer>> loaded = gson.fromJson(reader, typeOfMap);
                if (loaded != null) {
                    clientOffsets.clear();
                    clientOffsets.putAll(loaded);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
    private void saveAll() {
        saveQueueDataToDisk();
        saveClientOffsetsToDisk();
    }
    private void saveQueueDataToDisk() {
        try (Writer writer = new FileWriter(QUEUE_DATA_FILE)) {
            gson.toJson(queueData, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private void saveClientOffsetsToDisk() {
        try (Writer writer = new FileWriter(CLIENT_OFFSETS_FILE)) {
            gson.toJson(clientOffsets, writer);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}