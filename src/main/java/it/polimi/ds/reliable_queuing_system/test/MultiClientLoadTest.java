package it.polimi.ds.reliable_queuing_system.test;

import it.polimi.ds.reliable_queuing_system.utils.Address;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static it.polimi.ds.reliable_queuing_system.test.LoadTestClient.maxPoolSize;

public class MultiClientLoadTest {
    private final Address brokerAddress;
    private final int clientCount;
    private final int operationsPerClient;
    private final int threadsPerClient;
    private final double readWriteRatio;
    private final String[] queueIds;
    private final boolean verboseLogging;

    private final List<LoadTestClient> clients = new ArrayList<>();
    private final AtomicInteger totalSuccessfulOperations = new AtomicInteger(0);
    private final AtomicInteger totalFailedOperations = new AtomicInteger(0);
    private final AtomicLong totalLatency = new AtomicLong(0);

    public MultiClientLoadTest(
            Address brokerAddress,
            int clientCount,
            int operationsPerClient,
            int threadsPerClient,
            double readWriteRatio,
            String[] queueIds,
            boolean verboseLogging) {
        this.brokerAddress = brokerAddress;
        this.clientCount = clientCount;
        this.operationsPerClient = operationsPerClient;
        this.threadsPerClient = threadsPerClient;
        this.readWriteRatio = readWriteRatio;
        this.queueIds = queueIds;
        this.verboseLogging = verboseLogging;
    }

    public void runTest() throws InterruptedException {
        System.out.println("Starting multi-client load test with " + clientCount + " clients");

        // Create all client instances with unique IDs and ports
        for (int i = 0; i < clientCount; i++) {
            String clientIp = obtainClientIp();
            int clientPort = findAvailablePort();
            Address clientAddress = new Address(clientIp, clientPort);

            // Each client gets a unique ID
            int clientId = ThreadLocalRandom.current().nextInt(10000, 100000);

            LoadTestClient client = new LoadTestClient(
                    clientAddress,
                    brokerAddress,
                    clientId,
                    operationsPerClient,
                    threadsPerClient,
                    120, // max duration of the test in seconds
                    readWriteRatio,
                    queueIds,
                    1, // min value
                    1000, // max value
                    200, // operation delay (200 mean 5 ops/sec per client)
                    verboseLogging,
                    maxPoolSize// connection pool size

            );

            clients.add(client);
            System.out.println("Created client " + (i+1) + " with ID: " + clientId);
        }

        // CountDownLatch to wait for all clients to complete
        CountDownLatch clientsCompletionLatch = new CountDownLatch(clientCount);

        long startTime = System.currentTimeMillis();

        // Start all clients
        for (LoadTestClient client : clients) {
            new Thread(() -> {
                try {
                    client.startTest();

                    // Aggregate the results
                    totalSuccessfulOperations.addAndGet(client.getSuccessfulOperations());
                    totalFailedOperations.addAndGet(client.getFailedOperations());
                    totalLatency.addAndGet(client.getTotalLatency());


                    clientsCompletionLatch.countDown();
                } catch (InterruptedException e) {
                    System.err.println("Client test interrupted: " + e.getMessage());
                    clientsCompletionLatch.countDown();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }).start();

            try {
                Thread.sleep(100); // Start  10  clients per second
            } catch (InterruptedException e) {
                System.err.println("Startup sequence interrupted");
            }
        }

        // Wait for all clients to complete
        clientsCompletionLatch.await();
        long endTime = System.currentTimeMillis();

        // Print aggregate results
        int totalOps = clientCount * operationsPerClient;
        System.out.println("\n===== MULTI-CLIENT LOAD TEST RESULTS =====");
        System.out.println("Test duration: " + (endTime - startTime) / 1000.0 + " seconds");
        System.out.println("Number of clients: " + clientCount);
        System.out.println("Total operations requested: " + totalOps);
        System.out.println("Operations completed: " + totalSuccessfulOperations.get());
        System.out.println("Operations failed: " + totalFailedOperations.get());
        System.out.println("Operations timed out: " + (totalOps - totalSuccessfulOperations.get() - totalFailedOperations.get()));
        System.out.println("Throughput: " + (totalSuccessfulOperations.get() * 1000.0 / (endTime - startTime)) + " ops/sec");

        if (totalSuccessfulOperations.get() > 0) {
            System.out.println("Average latency: " + (totalLatency.get() / totalSuccessfulOperations.get()) + " ms");
        }
    }

    public static void main(String[] args) {
        try {
            System.out.println("======== RELIABLE QUEUING SYSTEM: MULTI-CLIENT LOAD TEST ========");

            Scanner scanner = new Scanner(System.in);

            System.out.print("Enter broker address (ip:port) [default: 127.0.0.1:8080]: ");
            String brokerInput = scanner.nextLine().trim();
            Address brokerAddress = brokerInput.isEmpty() ?
                    new Address("127.0.0.1", 8080) :
                    parseAddress(brokerInput);
            System.out.print("Enter connection pool size per client [default: 10]: ");
            String poolInput = scanner.nextLine().trim();
            int poolSize = poolInput.isEmpty() ? 10 : Integer.parseInt(poolInput);

            System.out.print("Enter number of clients [default: 5]: ");
            String clientsInput = scanner.nextLine().trim();
            int clientCount = clientsInput.isEmpty() ? 5 : Integer.parseInt(clientsInput);

            System.out.print("Enter operations per client [default: 200]: ");
            String opsInput = scanner.nextLine().trim();
            int operationsPerClient = opsInput.isEmpty() ? 200 : Integer.parseInt(opsInput);

            System.out.print("Enter threads per client [default: 2]: ");
            String threadsInput = scanner.nextLine().trim();
            int threadsPerClient = threadsInput.isEmpty() ? 2 : Integer.parseInt(threadsInput);

            System.out.print("Enter read/write ratio (0-1, 0=all writes, 1=all reads) [default: 0.3]: ");
            String ratioInput = scanner.nextLine().trim();
            double readWriteRatio = ratioInput.isEmpty() ? 0.3 : Double.parseDouble(ratioInput);

            System.out.print("Enter queue IDs separated by commas [default: queue1,queue2,queue3,queue4,queue5]: ");
            String queueInput = scanner.nextLine().trim();
            String[] queueIds = queueInput.isEmpty() ?
                    new String[]{"queue1", "queue2", "queue3", "queue4", "queue5"} :
                    queueInput.split(",");

            System.out.print("Enable verbose logging? (y/n) [default: n]: ");
            String verboseInput = scanner.nextLine().trim();
            boolean verboseLogging = verboseInput.equalsIgnoreCase("y");

            MultiClientLoadTest multiTest = new MultiClientLoadTest(
                    brokerAddress,
                    clientCount,
                    operationsPerClient,
                    threadsPerClient,
                    readWriteRatio,
                    queueIds,
                    verboseLogging
            );

            multiTest.runTest();

        } catch (Exception e) {
            System.err.println("Error in multi-client load test: " + e.getMessage());
            e.printStackTrace();
        }
    }

    // Utility methods (same as in LoadTestClient)
    private static String obtainClientIp() {
        try {
            return java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (java.net.UnknownHostException e) {
            return "127.0.0.1";
        }
    }

    private static int findAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            return 8090; // Fallback to a default port
        }
    }

    private static Address parseAddress(String addressString) {
        String[] parts = addressString.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid address format. Use <ip>:<port>");
        }
        return new Address(parts[0], Integer.parseInt(parts[1]));
    }
}