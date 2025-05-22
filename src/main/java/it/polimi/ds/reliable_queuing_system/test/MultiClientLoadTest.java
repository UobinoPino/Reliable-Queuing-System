//package it.polimi.ds.reliable_queuing_system.test;
//
//import it.polimi.ds.reliable_queuing_system.utils.Address;
//
//import java.io.IOException;
//import java.net.ServerSocket;
//import java.util.ArrayList;
//import java.util.Collections;
//import java.util.List;
//import java.util.Scanner;
//import java.util.concurrent.*;
//import java.util.concurrent.atomic.AtomicInteger;
//import java.util.concurrent.atomic.AtomicLong;
//
//
//
//public class MultiClientLoadTest {
//    private final Address brokerAddress;
//    private final int clientCount;
//    private final int operationsPerClient;
//    private final double readWriteRatio;
//    private final String[] queueIds;
//    private final boolean verboseLogging;
//
//    private final List<LoadTestClient> clients = new ArrayList<>();
//    private final AtomicInteger totalSuccessfulOperations = new AtomicInteger(0);
//    private final AtomicInteger totalFailedOperations = new AtomicInteger(0);
//    private final AtomicLong totalLatency = new AtomicLong(0);
//
//    public MultiClientLoadTest(
//            Address brokerAddress,
//            int clientCount,
//            int operationsPerClient,
//            double readWriteRatio,
//            String[] queueIds,
//            boolean verboseLogging) {
//        this.brokerAddress = brokerAddress;
//        this.clientCount = clientCount;
//        this.operationsPerClient = operationsPerClient;
//        this.readWriteRatio = readWriteRatio;
//        this.queueIds = queueIds;
//        this.verboseLogging = verboseLogging;
//    }
//
//    public void runTest() throws InterruptedException {
//        System.out.println("Starting multi-client load test with " + clientCount + " clients");
//
//        // Create all client instances with unique IDs and ports
//        for (int i = 0; i < clientCount; i++) {
//            String clientIp = obtainClientIp();
//            int clientPort = findAvailablePort();
//            Address clientAddress = new Address(clientIp, clientPort);
//
//            int clientId = ThreadLocalRandom.current().nextInt(10000, 100000);
//
//            LoadTestClient client = new LoadTestClient(
//                    clientAddress,
//                    brokerAddress,
//                    clientId,
//                    operationsPerClient,
//                    360,       // max duration of the test in seconds
//                    readWriteRatio,
//                    queueIds,
//                    1,         // min value
//                    1000,      // max value
//                    200,       // operation delay (200 mean 5 ops/sec per client)
//                    verboseLogging,
//                    200, // connection pool size
//                    200
//            );
//
//            clients.add(client);
//            System.out.println("Created client " + (i + 1) + " with ID: " + clientId);
//        }
//
//        CountDownLatch clientsCompletionLatch = new CountDownLatch(clientCount);
//        ExecutorService executor = Executors.newFixedThreadPool(clientCount);
//
//        final List<Long> clientStartTimes = Collections.synchronizedList(new ArrayList<>());
//        final List<Long> clientEndTimes = Collections.synchronizedList(new ArrayList<>());
//        final AtomicLong totalClientRuntime = new AtomicLong(0);
//
//        long overallStartTime = System.currentTimeMillis();
//        // Start all clients via executor
//        for (LoadTestClient client : clients) {
//            executor.submit(() -> {
//                try {
//
//                    long clientStartTime = System.currentTimeMillis();
//                    clientStartTimes.add(clientStartTime);
//                    client.startTest();
//
//                    long clientEndTime = System.currentTimeMillis();
//                    clientEndTimes.add(clientEndTime);
//
//                    // Calculate and add this client's runtime
//                    long clientRuntime = clientEndTime - clientStartTime;
//                    if (!client.hasClientIdRequestFailed()) {
//                        totalClientRuntime.addAndGet(clientRuntime);
//                        totalSuccessfulOperations.addAndGet(client.getSuccessfulOperations());
//                        totalFailedOperations.addAndGet(client.getFailedOperations());
//                        totalLatency.addAndGet(client.getTotalLatency());
//                    } else {
//                        System.out.println("Client " + client.getClientId() + " failed to acquire a client ID - excluding from statistics");
//                    }
//                } catch (InterruptedException e) {
//                    System.err.println("Client test interrupted: " + e.getMessage());
//                } catch (Exception e) {
//                    throw new RuntimeException(e);
//                } finally {
//                    clientsCompletionLatch.countDown();
//                }
//            });
//            try {
//                Thread.sleep(500); // Start 2 clients per second
//            } catch (InterruptedException e) {
//                System.err.println("Startup sequence interrupted");
//            }
//
//        }
//
//        // Wait for all clients to complete
//        clientsCompletionLatch.await();
//        long overallEndTime = System.currentTimeMillis();
//
//        executor.shutdown();
//
//        // Print aggregate results
//        int totalOps = clientCount * operationsPerClient;
//        double overallDuration = (overallEndTime - overallStartTime) / 1000.0;
//        double avgClientDuration = clientCount > 0 ? totalClientRuntime.get() / (clientCount * 1000.0) : 0;
//        System.out.println("\n===== MULTI-CLIENT LOAD TEST RESULTS =====");
//        System.out.println("Test duration: " + overallDuration +  " seconds");
//        System.out.println("Average client runtime: " + avgClientDuration + " seconds");
//        System.out.println("Number of clients: " + clientCount);
//        System.out.println("Total operations requested: " + totalOps);
//        System.out.println("Operations completed: " + totalSuccessfulOperations.get());
//        System.out.println("Operations failed: " + totalFailedOperations.get());
//        System.out.println("Operations timed out: " +
//                (totalOps - totalSuccessfulOperations.get() - totalFailedOperations.get()));
//        double effectiveThroughput = totalClientRuntime.get() > 0 ?
//                (totalSuccessfulOperations.get() * 1000.0 / totalClientRuntime.get()) * clientCount : 0;
//        System.out.println("Effective throughput: " + effectiveThroughput + " ops/sec");
//        System.out.println("Overall Throughput: " +
//                (totalSuccessfulOperations.get() * 1000.0 / (overallEndTime - overallStartTime)) + " ops/sec");
//
//        if (totalSuccessfulOperations.get() > 0) {
//            System.out.println("Average latency: " +
//                    (totalLatency.get() / totalSuccessfulOperations.get()) + " ms");
//        }
//    }
//
//    public static void main(String[] args) {
//        try {
//            System.out.println("======== RELIABLE QUEUING SYSTEM: MULTI-CLIENT LOAD TEST ========");
//
//            Scanner scanner = new Scanner(System.in);
//
//            System.out.print("Enter broker address (ip:port) [default: 127.0.0.1:5001]: ");
//            String brokerInput = scanner.nextLine().trim();
//            Address brokerAddress = brokerInput.isEmpty() ?
//                    new Address("127.0.0.1", 5001) :
//                    parseAddress(brokerInput);
//
//
//            System.out.print("Enter number of clients [default: 10]: ");
//            String clientsInput = scanner.nextLine().trim();
//            int clientCount = clientsInput.isEmpty() ? 10 : Integer.parseInt(clientsInput);
//
//            System.out.print("Enter operations per client [default: 100]: ");
//            String opsInput = scanner.nextLine().trim();
//            int operationsPerClient = opsInput.isEmpty() ? 100 : Integer.parseInt(opsInput);
//
//
//            System.out.print("Enter read/write ratio (0-1, 0=all writes, 1=all reads) [default: 0.3]: ");
//            String ratioInput = scanner.nextLine().trim();
//            double readWriteRatio = ratioInput.isEmpty() ? 0.3 : Double.parseDouble(ratioInput);
//
//            System.out.print("Enter queue IDs separated by commas [default: queue1,queue2,queue3,queue4,queue5]: ");
//            String queueInput = scanner.nextLine().trim();
//            String[] queueIds = queueInput.isEmpty() ?
//                    new String[]{"queue1", "queue2", "queue3", "queue4", "queue5"} :
//                    queueInput.split(",");
//
//            System.out.print("Enable verbose logging? (y/n) [default: n]: ");
//            String verboseInput = scanner.nextLine().trim();
//            boolean verboseLogging = verboseInput.equalsIgnoreCase("y");
//
//            MultiClientLoadTest multiTest = new MultiClientLoadTest(
//                    brokerAddress,
//                    clientCount,
//                    operationsPerClient,
//                    readWriteRatio,
//                    queueIds,
//                    verboseLogging
//            );
//
//            multiTest.runTest();
//        } catch (Exception e) {
//            System.err.println("Error in multi-client load test: " + e.getMessage());
//            e.printStackTrace();
//        }
//    }
//
//    // Utility methods (same as in LoadTestClient)
//    private static String obtainClientIp() {
//        try {
//            return java.net.InetAddress.getLocalHost().getHostAddress();
//        } catch (java.net.UnknownHostException e) {
//            return "127.0.0.1";
//        }
//    }
//
//    private static int findAvailablePort() {
//        try (ServerSocket socket = new ServerSocket(0)) {
//            return socket.getLocalPort();
//        } catch (IOException e) {
//            return 8090; // Fallback to a default port
//        }
//    }
//
//    private static Address parseAddress(String addressString) {
//        String[] parts = addressString.split(":");
//        if (parts.length != 2) {
//            throw new IllegalArgumentException("Invalid address format. Use <ip>:<port>");
//        }
//        return new Address(parts[0], Integer.parseInt(parts[1]));
//    }
//}