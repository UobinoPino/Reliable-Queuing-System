package it.polimi.ds.reliable_queuing_system.test;

import it.polimi.ds.reliable_queuing_system.messages.*;
import it.polimi.ds.reliable_queuing_system.utils.Address;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class LoadTestClient {
    private final Address clientAddress;
    private final Address brokerAddress;
    private Integer clientId;
    private final CountDownLatch clientIdLatch = new CountDownLatch(1);
    private final AtomicInteger nextOperationId = new AtomicInteger(0);
    private final AtomicInteger successfulOperations = new AtomicInteger(0);
    private final AtomicInteger failedOperations = new AtomicInteger(0);
    private final AtomicLong totalLatency = new AtomicLong(0);
    //private final ConcurrentHashMap<Integer, Long> pendingOperations = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<Integer, OperationDetail> pendingOperations = new ConcurrentHashMap<>();
    private final CountDownLatch completionLatch;
    private final ExecutorService executorService;
    private final ConcurrentHashMap<String, AtomicInteger> queueWrites = new ConcurrentHashMap<>();
    private volatile boolean running = true;
    private final ExecutorService processingPool;

    // Connection pool settings
    private int maxPoolSize = 10;

    private static final int CONNECTION_TIMEOUT = 30000;
    private static final int SOCKET_TIMEOUT = 30000;

    // Direct config variables
    private final int totalOperations;
    private final int concurrentThreads;
    private final int concurrentOperations;
    private final long testTimeoutSeconds;
    private final double readWriteRatio;
    private final String[] queueIds;
    private final int minValue;
    private final int maxValue;
    private long operationDelayMs;
    private final boolean verboseLogging;

    public LoadTestClient(
            Address clientAddress,
            Address brokerAddress,
            Integer clientId,
            int totalOperations,
            int concurrentThreads,
            long testTimeoutSeconds,
            double readWriteRatio,
            String[] queueIds,
            int minValue,
            int maxValue,
            long operationDelayMs,
            boolean verboseLogging,
            int maxPoolSize,
            int concurrentOperations) {
        this.clientAddress = clientAddress;
        this.brokerAddress = brokerAddress;
        this.clientId = clientId;
        this.totalOperations = totalOperations;
        this.concurrentThreads = concurrentThreads;
        this.testTimeoutSeconds = testTimeoutSeconds;
        this.readWriteRatio = readWriteRatio;
        this.queueIds = queueIds;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.operationDelayMs = operationDelayMs;
        this.verboseLogging = verboseLogging;
        this.maxPoolSize = maxPoolSize;
        this.completionLatch = new CountDownLatch(totalOperations);
        this.executorService = Executors.newFixedThreadPool(concurrentThreads);
        this.concurrentOperations = concurrentOperations;
        this.processingPool = Executors.newFixedThreadPool(concurrentOperations);

        // Pre-populate queue writes tracker with all queue IDs
        for (String queueId : queueIds) {
            queueWrites.put(queueId, new AtomicInteger(0));
        }
    }

    private static class OperationDetail {
        final long startTime;
        final String type;
        final String queueName;
        final Integer value;  // null for reads
        final int clientId;
        final Address clientAddress;

        OperationDetail(String type, String queueName, Integer value, int clientId, Address clientAddress) {
            this.startTime = System.currentTimeMillis();
            this.type = type;
            this.queueName = queueName;
            this.value = value;
            this.clientId = clientId;
            this.clientAddress = clientAddress;
        }
        @Override
        public String toString() {
            if ("write".equals(type)) {
                return "WriteRequest[queueName=" + queueName + ", value=" + value +
                        ", clientId=" + clientId + ", operationId=%d, clientAddress=" + clientAddress + "]";
            } else {
                return "ReadRequest[queueName=" + queueName +
                        ", clientId=" + clientId + ", operationId=%d, clientAddress=" + clientAddress + "]";
            }
        }
    }

    private static class PooledConnection {
        Socket socket;
        ObjectOutputStream outputStream;
        ObjectInputStream inputStream;

        public PooledConnection(Socket socket) throws IOException {
            this.socket = socket;
            this.outputStream = new ObjectOutputStream(socket.getOutputStream());
            this.outputStream.flush(); // Important: flush the header immediately
        }

        public ObjectInputStream getInputStream() throws IOException {
            if (inputStream == null) {
                inputStream = new ObjectInputStream(socket.getInputStream());
            }
            return inputStream;
        }

        public void close() {
            try {
                if (socket != null && !socket.isClosed()) {
                    socket.close();
                }
            } catch (IOException ignored) {}
        }
    }

    private final BlockingQueue<PooledConnection> connectionPool = new LinkedBlockingQueue<>(maxPoolSize);

    private void closeAllConnections() {
        PooledConnection conn;
        while ((conn = connectionPool.poll()) != null) {
            conn.close();
        }
    }
    private void requestClientId() throws IOException, InterruptedException {
        PooledConnection conn = getConnection();
        try {
            conn.outputStream.writeObject(new ClientIdRequest(clientAddress));
            conn.outputStream.flush();
        } finally {
            returnConnection(conn);
        }
        if (!clientIdLatch.await(CONNECTION_TIMEOUT, TimeUnit.MILLISECONDS)) {
            throw new RuntimeException("Timed out waiting for client ID");
        }
    }

    public void startTest() throws InterruptedException, IOException {
        System.out.println("Starting load test with configuration: " + getConfigString());

        // Start message listener
        Thread listenerThread = new Thread(this::incomingMessagesListener);
        listenerThread.setDaemon(true);
        listenerThread.start();

        // Wait for listener to start
        Thread.sleep(500);

        long startTime = System.currentTimeMillis();

        requestClientId();

        // Submit operations to the thread pool
        for (int i = 0; i < totalOperations; i++) {
            final int operationIndex = i;
            executorService.submit(() -> {
                try {
                    boolean isRead = shouldPerformRead();
                    String queueId = selectQueue();

                    if (isRead) {
                        performRead(queueId);
                    } else {
                        Integer value = generateValue();
                        performWrite(queueId, value);
                        queueWrites.get(queueId).incrementAndGet();
                    }

                    // Add delay between operations if configured
                    if (operationDelayMs > 0) {
                        Thread.sleep(operationDelayMs);
                    }
                } catch (InterruptedException e) {
                    // we were shut down—no need to log
                    return;
                }catch (Exception e) {
                    System.err.println("Error performing operation " + operationIndex + ": " + e.getMessage());
                    String errorType = e.getClass().getSimpleName();
                    String errorDetail = e.getMessage();
                    System.err.println("Error performing operation " + operationIndex +
                            " (" + errorType + "): " + errorDetail);

                    if (verboseLogging) {
                        e.printStackTrace();
                    }

                    if (e instanceof IOException) {
                        System.err.println("  Network error with broker at " + brokerAddress);
                    }

                    failedOperations.incrementAndGet();
                    completionLatch.countDown();
                }
            });
        }

        // Wait for all operations to complete
        boolean completed = completionLatch.await(testTimeoutSeconds, TimeUnit.SECONDS);

        // Test finished
        running = false;
        long endTime = System.currentTimeMillis();
        executorService.shutdownNow();

        try {
            if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException ie) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
        processingPool.shutdownNow();
        try {
            if (!processingPool.awaitTermination(1, TimeUnit.SECONDS)) {
                processingPool.shutdownNow();
            }
        } catch (InterruptedException ignored) {
            processingPool.shutdownNow();
            Thread.currentThread().interrupt();
        }
        closeAllConnections();

        // Print results
        System.out.println("\n===== LOAD TEST RESULTS =====");
        System.out.println("Test duration: " + (endTime - startTime) / 1000.0 + " seconds");
        System.out.println("Total operations requested: " + totalOperations);
        System.out.println("Operations completed: " + successfulOperations.get());
        System.out.println("Operations failed: " + failedOperations.get());
        System.out.println("Operations timed out: " + (totalOperations - successfulOperations.get() - failedOperations.get()));
        System.out.println("Throughput: " + (successfulOperations.get() * 1000.0 / (endTime - startTime)) + " ops/sec");

        if (successfulOperations.get() > 0) {
            System.out.println("Average latency: " + (totalLatency.get() / successfulOperations.get()) + " ms");
        }

        System.out.println("\nWrite operations per queue:");
        for (Map.Entry<String, AtomicInteger> entry : queueWrites.entrySet()) {
            System.out.println("  Queue " + entry.getKey() + ": " + entry.getValue().get() + " writes");
        }
        // Log timed out operations
        if (!pendingOperations.isEmpty()) {
            System.out.println("\n===== TIMED OUT OPERATIONS =====");
            pendingOperations.forEach((opId, detail) -> {
                System.out.println(String.format(detail.toString(), opId));
            });
            System.out.println("Total timed out: " + pendingOperations.size());
        }
    }

    private String getConfigString() {
        return "LoadTestConfig{" +
                "totalOperations=" + totalOperations +
                ", concurrentThreads=" + concurrentThreads +
                ", testTimeoutSeconds=" + testTimeoutSeconds +
                ", readWriteRatio=" + readWriteRatio +
                ", queueIds=" + Arrays.toString(queueIds) +
                ", valueRange=[" + minValue + "-" + maxValue + "]" +
                ", operationDelayMs=" + operationDelayMs +
                ", verboseLogging=" + verboseLogging +
                ", maxPoolSize=" + maxPoolSize +
                '}';
    }

    private void incomingMessagesListener() {
        try (ServerSocket serverSocket = new ServerSocket(clientAddress.port())) {
            System.out.println("Load test client ready to receive messages at port: " + clientAddress.port());

            while (running) {
                try {
                    Socket socket = serverSocket.accept();
                    socket.setSoTimeout(SOCKET_TIMEOUT);
                    CompletableFuture.runAsync(() -> handleIncomingConnection(socket));
                } catch (IOException e) {
                    if (running) {
                        System.err.println("Error accepting connection: " + e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Fatal error in message listener: " + e.getMessage());
        }
    }
    private void handleClientIdAssignment(ClientIdAssignment msg) {
        this.clientId = msg.clientId();
        clientIdLatch.countDown();
        System.out.println("Assigned client ID: " + clientId);
        sendAck(-1);
    }

    private void handleIncomingConnection(Socket socket) {
        try (ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {
            while (running && !socket.isClosed()) {
                try {
                    Message message = (Message) in.readObject();
                    // hand off processing immediately
                    processingPool.execute(() -> {
                        switch (message) {
                            case ClientIdAssignment msg -> handleClientIdAssignment(msg);
                            case ReadResponse msg       -> handleReadResponse(msg);
                            case ReadConfirmation msg   -> handleReadConfirmation(msg);
                            case WriteResponse msg      -> handleWriteResponse(msg);
                            default -> {
                                if (verboseLogging) {
                                    System.out.println("Received unexpected type: " + message.getClass().getSimpleName());
                                }
                            }
                        }
                    });
                } catch (SocketTimeoutException ste) {
                    // no data; retry
                } catch (EOFException | SocketException e) {
                    break; // connection closed
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException(e);
                }
            }
        } catch (IOException e) {
            if (verboseLogging) {
                System.err.println("Error in incoming connection: " + e.getMessage());
            }
        } finally {
            try { socket.close(); } catch (IOException ignore) {}
        }
    }

    private void sendAck(int operationId) {
        try {
            PooledConnection conn = getConnection();
            conn.outputStream.writeObject(new RequestCompletedAck(clientAddress, operationId));
            conn.outputStream.flush();
            returnConnection(conn);
        } catch (IOException ignored) {}
    }

    private void handleReadResponse(ReadResponse msg) {
        // Just logging for debugging if needed

//        if (verboseLogging) {
//            List<Integer> values = msg.values();
//            System.out.println("Read response received for operation " + msg.operationId() +
//                    ": " + (values.isEmpty() ? "no values" : values.size() + " values"));
//        }
    }

    private void handleReadConfirmation(ReadConfirmation msg) {
        OperationDetail detail  = pendingOperations.remove(msg.operationId());
        if (detail != null) {
            long latency = System.currentTimeMillis() - detail.startTime;
            totalLatency.addAndGet(latency);
            successfulOperations.incrementAndGet();
            completionLatch.countDown();
            System.out.println("Read confirmation received for operation " + msg.operationId() + " for client " + clientId);
            sendAck(msg.operationId());


            if (verboseLogging) {
                System.out.println("Read confirmed for operation " + msg.operationId() + " (latency: " + latency + " ms)");
            }
        }
    }

    private void handleWriteResponse(WriteResponse msg) {
        OperationDetail detail= pendingOperations.remove(msg.operationId());
        if (detail != null) {
            long latency = System.currentTimeMillis() - detail.startTime;
            totalLatency.addAndGet(latency);
            successfulOperations.incrementAndGet();
            completionLatch.countDown();
            System.out.println("Write response received for operation " + msg.operationId()+ " for client " + clientId);
            sendAck(msg.operationId());

            if (verboseLogging) {
                System.out.println("Write confirmed for operation " + msg.operationId() + " (latency: " + latency + " ms)");
            }
        }
    }

    private PooledConnection getConnection() throws IOException {
        PooledConnection connection = connectionPool.poll();

        // Validate connection before using
        if (connection != null) {
            if (!connection.socket.isConnected() || connection.socket.isClosed()) {
                connection.close();
                connection = null;
            }
        }

        // Create new connection if needed
        if (connection == null) {
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(brokerAddress.ip(), brokerAddress.port()), CONNECTION_TIMEOUT);
            connection = new PooledConnection(socket);
        }

        return connection;
    }

    private void returnConnection(PooledConnection connection) {
        if (connection != null && connection.socket.isConnected() &&
                !connection.socket.isClosed()) {
            // Return to pool or close based on pool size
            if (!connectionPool.offer(connection)) {
                connection.close();
            }
        } else if (connection != null) {
            connection.close();
        }
    }

    private void performRead(String queueId) throws IOException {
        PooledConnection connection = null;
        try {
            connection = getConnection();
            int operationId = nextOperationId.getAndIncrement();

            // Send request using cached output stream
            connection.outputStream.writeObject(new ReadRequest(queueId, clientId, operationId, clientAddress));
            connection.outputStream.reset(); // Reset object cache to prevent memory leaks
            connection.outputStream.flush();

            pendingOperations.put(operationId, new OperationDetail("read", queueId, null, clientId, clientAddress));   ;

            if (verboseLogging) {
                System.out.println("Sent read request for queue " + queueId + " (operation " + operationId + ")");
            }
        } finally {
            returnConnection(connection);
        }
    }

    private void performWrite(String queueId, Integer value) throws IOException {
        PooledConnection connection = null;
        try {
            connection = getConnection();
            int operationId = nextOperationId.getAndIncrement();

            // Send request using cached output stream
            connection.outputStream.writeObject(new WriteRequest(queueId, value, clientId, operationId, clientAddress));
            connection.outputStream.reset(); // Reset object cache to prevent memory leaks
            connection.outputStream.flush();

            pendingOperations.put(operationId, new OperationDetail("write", queueId, value, clientId, clientAddress));

            if (verboseLogging) {
                System.out.println("Sent write request with value " + value + " to queue " + queueId +
                        " (operation " + operationId + ")");
            }
        } finally {
            returnConnection(connection);
        }
    }

    private boolean shouldPerformRead() {
        return ThreadLocalRandom.current().nextDouble() < readWriteRatio;
    }

    private String selectQueue() {
        int queueIndex = ThreadLocalRandom.current().nextInt(queueIds.length);
        return queueIds[queueIndex];
    }

    private Integer generateValue() {
        return ThreadLocalRandom.current().nextInt(minValue, maxValue + 1);
    }

    public static void main(String[] args) {
        try {
            System.out.println("======== RELIABLE QUEUING SYSTEM: LOAD TEST CLIENT ========");

            // Interactive configuration for easier use in IntelliJ
            Scanner scanner = new Scanner(System.in);

            System.out.print("Enter broker address (ip:port) [default: 127.0.0.1:8080]: ");
            String brokerInput = scanner.nextLine().trim();
            Address brokerAddress = brokerInput.isEmpty() ?
                    new Address("127.0.0.1", 5001) :
                    parseAddress(brokerInput);

            System.out.print("Enter connection pool size per client [default: 10]: ");
            String poolInput = scanner.nextLine().trim();
            int poolSize = poolInput.isEmpty() ? 10 : Integer.parseInt(poolInput);

            System.out.print("Enter total number of operations [default: 1000]: ");
            String opsInput = scanner.nextLine().trim();
            int totalOperations = opsInput.isEmpty() ? 1000 : Integer.parseInt(opsInput);

            System.out.print("Enter number of concurrent threads [default: 1]: ");
            String threadsInput = scanner.nextLine().trim();
            int concurrentThreads = threadsInput.isEmpty() ? 1 : Integer.parseInt(threadsInput);

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

            // Create client address
            String clientIp = obtainClientIp();
            int clientPort = findAvailablePort();
            Address clientAddress = new Address(clientIp, clientPort);

            // Use a random client ID
            int clientId = ThreadLocalRandom.current().nextInt(10000, 100000);

            System.out.println("Client ID: " + clientId);
            System.out.println("Client Address: " + clientAddress);
            System.out.println("Broker Address: " + brokerAddress);

            // Create and start load test with direct parameters
            LoadTestClient loadTestClient = new LoadTestClient(
                    clientAddress,
                    brokerAddress,
                    clientId,
                    totalOperations,
                    concurrentThreads,
                    180,  // max duration in seconds of the simulation
                    readWriteRatio,
                    queueIds,
                    1,    // min value
                    1000, // max value
                    250,   // operation delay ms
                    verboseLogging,
                    poolSize , // max pool size
                    20 // concurrent operations
            );

            loadTestClient.startTest();

        } catch (Exception e) {
            System.err.println("Error in load test: " + e.getMessage());
            e.printStackTrace();
        }
    }

    private static String obtainClientIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
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

    public int getSuccessfulOperations() {
        return successfulOperations.get();
    }

    public int getFailedOperations() {
        return failedOperations.get();
    }

    public long getTotalLatency() {
        return totalLatency.get();
    }
}
