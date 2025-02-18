package distributedqueue;

import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

/**
 * Manages persistence of the queues on disk.
 *
 * Requirements addressed:
 * 11 (stable storage), 12 (supports recovery), 14 (persist client offsets).
 */
public class QueueManager {

    private static final String QUEUE_DIR = "queues";
    private static final String OFFSET_DIR = "offsets";

    public QueueManager() {
        File queueDir = new File(QUEUE_DIR);
        if (!queueDir.exists()) {
            queueDir.mkdirs();
        }
        File offsetDir = new File(OFFSET_DIR);
        if (!offsetDir.exists()) {
            offsetDir.mkdirs();
        }
    }

    public void createQueueOnDisk(String queueName) {
        File queueFile = new File(QUEUE_DIR, queueName + ".data");
        try {
            if (!queueFile.exists()) {
                queueFile.createNewFile();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void appendDataToDisk(String queueName, int data) {
        File queueFile = new File(QUEUE_DIR, queueName + ".data");
        try (FileWriter fw = new FileWriter(queueFile, true)) {
            fw.write(data + "\n");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void persistClientOffsetToDisk(String queueName, String clientId, int offset) {
        File offsetFile = new File(OFFSET_DIR, queueName + "_" + clientId + ".offset");
        try (FileWriter fw = new FileWriter(offsetFile, false)) {
            fw.write(String.valueOf(offset));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public int readClientOffsetFromDisk(String queueName, String clientId) {
        File offsetFile = new File(OFFSET_DIR, queueName + "_" + clientId + ".offset");
        if (!offsetFile.exists()) {
            return 0;
        }
        try (BufferedReader br = new BufferedReader(new FileReader(offsetFile))) {
            String offsetStr = br.readLine();
            return offsetStr == null ? 0 : Integer.parseInt(offsetStr);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return 0;
    }

    /**
     * Load a queue from disk.
     * @param queueName The queue name.
     * @return The in-memory list of data.
     */
    public List<Integer> loadQueueData(String queueName) {
        File queueFile = new File(QUEUE_DIR, queueName + ".data");
        List<Integer> dataList = new ArrayList<>();
        if (!queueFile.exists()) {
            return dataList;
        }
        try (BufferedReader br = new BufferedReader(new FileReader(queueFile))) {
            br.lines().forEach(line -> {
                try {
                    dataList.add(Integer.parseInt(line));
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
        return dataList;
    }
}