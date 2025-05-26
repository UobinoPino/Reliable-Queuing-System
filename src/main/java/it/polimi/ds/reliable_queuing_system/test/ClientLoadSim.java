package it.polimi.ds.reliable_queuing_system.test;

import it.polimi.ds.reliable_queuing_system.messages.*;
import it.polimi.ds.reliable_queuing_system.utils.Address;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ClientLoadSim {
    //region PASSED PARAMETERS
    private final Address clientAddress;
    private final Address brokerAddress;
    private final int totalOperations;
    private final double readWriteRatio;
    private final String[] queueNames;
    //endregion

    //region SIMULATION-RELATED PARAMETERS
    private Integer clientId;
    private final Set<Integer> receivedResponses;
    private final List<Integer> completedOperations;
    private final List<Integer> failedOperations;
    private final AtomicInteger nextOperationId;
    private final Map<String, List<Integer>> writtenQueues;
    private final Map<Integer, String> readQueueNames;
    private final Map<Integer, List<Integer>> expectedValues;
    private final Map<Integer, List<Integer>> readValues;

    private final AtomicLong startTime;
    private final AtomicLong endTime;
    //endregion

    //region SOCKET-RELATED PARAMETERS
    private Socket outSocket;
    private ObjectOutputStream outStream;
    //endregion

    //region INTERNAL STATE PARAMETERS
    private final ExecutorService incomingSocketsExecutor;
    private final ExecutorService incomingMessagesExecutor;

    private CompletableFuture<Object> sentOperationTimeout;
    private CountDownLatch sentOperationCompletedLatch;
    private CountDownLatch incomingMessageListenerReadyLatch;
    private final AtomicBoolean checksStarted;
    private final AtomicBoolean simCompleted;
    //endregion

    //region CONSTANTS
    private static final int SOCKET_TIMEOUT_S = 60;
    private static final int OPERATION_TIMEOUT_S = 180;
    private static final int MIN_VALUE_TO_WRITE = 0;
    private static final int MAX_VALUE_TO_WRITE = 999;
//    private static final int OPERATIONS_PER_SECOND = 1;
    //endregion

    public ClientLoadSim(
            Address clientAddress,
            Address brokerAddress,
            int totalOperations,
            double readWriteRatio,
            String[] queueNames
    ) {
        this.clientAddress = clientAddress;
        this.brokerAddress = brokerAddress;
        this.totalOperations = totalOperations;
        this.readWriteRatio = readWriteRatio;
        this.queueNames = queueNames;

        this.clientId = null;
        this.receivedResponses = ConcurrentHashMap.newKeySet();
        this.completedOperations = new CopyOnWriteArrayList<>();
        this.failedOperations = new CopyOnWriteArrayList<>();
        this.nextOperationId = new AtomicInteger(0);
        this.writtenQueues = new ConcurrentHashMap<>();
        for (String queueName : queueNames) {
            writtenQueues.put(queueName, new CopyOnWriteArrayList<>());
        }
        this.readQueueNames = new ConcurrentHashMap<>();
        this.expectedValues = new ConcurrentHashMap<>();
        this.readValues = new ConcurrentHashMap<>();
        this.startTime = new AtomicLong();
        this.endTime = new AtomicLong();

        this.outSocket = null;
        this.outStream = null;

        this.incomingSocketsExecutor = Executors.newFixedThreadPool(100);
        this.incomingMessagesExecutor = Executors.newSingleThreadExecutor();

        this.sentOperationTimeout = null;
        this.sentOperationCompletedLatch = null;
        this.incomingMessageListenerReadyLatch = null;
        this.checksStarted = new AtomicBoolean(false);
        this.simCompleted = new AtomicBoolean(false);
    }

    public LoadSimResults start() throws InterruptedException {
        System.out.println("Starting Simulation of Client " + clientAddress);

        // Start the message listener
        incomingMessageListenerReadyLatch = new CountDownLatch(1);
        Thread listenerThread = new Thread(this::incomingMessagesListener);
        listenerThread.setDaemon(true);  //TODO: understand why (and if) needed
        listenerThread.start();

        // wait for the listener to start
        incomingMessageListenerReadyLatch.await();

        // register starting time
        startTime.set(System.currentTimeMillis());

        // request client ID
        requestClientId();

        // wait for client ID
        sentOperationCompletedLatch.await();

        // read from all queues (to be able to test if read and writes matches later)
        for (String queueName : queueNames) {
            performReadOperation(queueName);

            // wait for the operation to complete
            sentOperationCompletedLatch.await();
//            Thread.sleep(OPERATIONS_PER_SECOND / 1000);
        }

        // raise the flag to start checking for correctness of performed reads and writes
        checksStarted.set(true);

        for (int i = 0; i < totalOperations; i++) {
            // determine if the operation is a read or a write
            boolean isRead = shouldPerformRead();

            // determine the queue to use
            String queueName = selectQueue();

            // perform the determined operation
            if (isRead) {
                performReadOperation(queueName);
            } else {
                int valueToWrite = ThreadLocalRandom.current().nextInt(MIN_VALUE_TO_WRITE, MAX_VALUE_TO_WRITE + 1);
                performWriteOperation(queueName, valueToWrite);
            }

            // wait for the operation to complete
            sentOperationCompletedLatch.await();
//            Thread.sleep(OPERATIONS_PER_SECOND / 1000);
        }

        // register ending time
        endTime.set(System.currentTimeMillis());

        // mark the simulation as completed
        simCompleted.set(true);

        // shutdown the executors and the listener thread
        incomingMessagesExecutor.shutdownNow();
        incomingSocketsExecutor.shutdownNow();
        listenerThread.interrupt();

        // close the outgoing socket
        if (outSocket != null) {
            try {
                outStream.close();
                outSocket.close();
            } catch (Exception e) {
                System.out.println("[ERROR]: Error while closing the outgoing socket: " + e.getMessage());
            } finally {
                outSocket = null;
                outStream = null;
            }
        }

        // return the simulation results

        return new LoadSimResults(
                clientId,
                totalOperations,
                completedOperations.size(),
                failedOperations.size(),
                endTime.get() - startTime.get(),
                completedOperations.size() / ((endTime.get() - startTime.get()) / 1000.0)
        );
    }

    /// A method that will randomly return true or false based on the `readWriteRatio` parameter.
    private boolean shouldPerformRead() {
        return ThreadLocalRandom.current().nextDouble() < readWriteRatio;
    }

    /// A method that will return a random queueName out of the ones specified in the `queueNames` parameter.
    private String selectQueue() {
        int index = ThreadLocalRandom.current().nextInt(queueNames.length);
        return queueNames[index];
    }

    private void incomingMessagesListener() {
        try(ServerSocket serverSocket = new ServerSocket(clientAddress.port())) {
            System.out.println("Load test client ready to receive messages at port: " + clientAddress.port());

            incomingMessageListenerReadyLatch.countDown();

            while (!simCompleted.get() && !serverSocket.isClosed()) {
                try {
                    Socket socket = serverSocket.accept();
                    socket.setSoTimeout(SOCKET_TIMEOUT_S * 1000);

                    incomingSocketsExecutor.execute(() -> handleIncomingConnection(socket));
                }
                catch (IOException e) {
                    if (!simCompleted.get()) {
                        System.out.println("[ERROR]: Unable to accept incoming socket connection: " + e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            System.out.println("[ERROR]: Unable to create server socket: " + e.getMessage());
        }
    }

    private void handleIncomingConnection(Socket inSocket) {
        try (ObjectInputStream inStream = new ObjectInputStream(inSocket.getInputStream())) {
            // listen for incoming messages
            while (!simCompleted.get() && !inSocket.isClosed()) {
                try {
                    Message message = (Message) inStream.readObject();

                    incomingMessagesExecutor.execute(() -> {
                        switch (message) {
                            case ClientIdAssignment msg -> handleClientIdAssignment(msg);
                            case ReadResponse msg       -> handleReadResponse(msg);
                            case WriteResponse msg      -> handleWriteResponse(msg);
                            default -> System.out.println("[INFO]: Non-client message received.");
                        }
                    });
                } catch (ClassNotFoundException | ClassCastException e) {
                    System.out.println("[INFO]: Unknown message received: " + e.getMessage());
                } catch (IOException ignored) {
                    break;
                }
            }
            inSocket.close();
        } catch (IOException e) {
            System.out.println("[ERROR]: Unable to create ObjectInputStream: " + e.getMessage());
        }
    }

    private void handleClientIdAssignment(ClientIdAssignment msg) {
        if (!completedOperations.add(-1)) {
            System.out.println("[INFO]: Duplicated ClientIdAssignment received for client " + clientAddress + ". Ignoring.");
            return;
        }

        System.out.println("[INFO]: Client " + clientAddress + " received client ID assignment: " + msg.clientId());

        // register the obtained client ID
        clientId = msg.clientId();

        // send ack to the broker
        sendAck(-1);

        // cancel timeout and notify the latch
        sentOperationTimeout.complete(null);
        sentOperationCompletedLatch.countDown();
    }

    private void handleReadResponse(ReadResponse msg) {
        if (!receivedResponses.add(msg.operationId())) {
            System.out.println("[INFO]: Duplicated ReadResponse received for operation " + msg.operationId() + ". Ignoring.");
            return;
        }

        System.out.println("[INFO]: Client " + clientAddress + " received ReadResponse for operation " + msg.operationId());

        // store the read values
        if (checksStarted.get()) {
            readQueueNames.put(msg.operationId(), msg.queueName());
            expectedValues.put(msg.operationId(), new CopyOnWriteArrayList<>(writtenQueues.get(msg.queueName())));
            readValues.put(msg.operationId(), new CopyOnWriteArrayList<>(msg.values()));
        }

        // check if the read values are coherent with the written ones
        if (checksStarted.get()) {
            boolean success;
            String queueName = readQueueNames.get(msg.operationId());
            List<Integer> exp = expectedValues.get(msg.operationId());
            List<Integer> read = readValues.get(msg.operationId());
            if (exp == null) {
                success = false;
                System.out.println("[ERROR]: Read confirmation received for queue " + queueName + " but the queue does not exist for client " + clientId);
            }
            else if (exp.size() != read.size()) {
                success = false;
                System.out.println("[ERROR]: Read confirmation received for queue " + queueName + " but the number of values does not match for client " + clientId);
                System.out.println(exp);
                System.out.println(read);
            }
            else {
                success = exp.equals(read);
            }

            if (success) {
                // register the completed operation
                completedOperations.add(msg.operationId());

                // remove the already red values from the expected queue (so that they won't be looked for in future reads)
                for (int i = 0; i < read.size(); i++) {
                    writtenQueues.get(queueName).removeFirst();
                }
            }
            else {
                // register the failed operation
                failedOperations.add(msg.operationId());
            }
        }

        // send ack to the broker
        sendAck(msg.operationId());

        // cancel timeout aWnd notify the latch
        sentOperationTimeout.complete(null);
        sentOperationCompletedLatch.countDown();
    }

    private void handleWriteResponse(WriteResponse msg) {
        // ignore duplicated responses
        if (!receivedResponses.add(msg.operationId())) {
            System.out.println("[INFO]: Duplicated WriteResponse received for operation " + msg.operationId() + ". Ignoring.");
            return;
        }

        System.out.println("[INFO]: Client " + clientAddress + " received WriteResponse for operation " + msg.operationId());

        // register the completed operation
        if (checksStarted.get()) {
            completedOperations.add(msg.operationId());
        }

        // send ack to the broker
        sendAck(msg.operationId());

        // cancel timeout and notify the latch
        sentOperationTimeout.complete(null);
        sentOperationCompletedLatch.countDown();
    }

    private void requestClientId() {
        try {
            // ensure the outgoing socket exists
            ensureOutSocketExists();

            // send the request
            outStream.writeObject(new ClientIdRequest(clientAddress, brokerAddress));
            outStream.flush();

            System.out.println("[INFO]: Client " + clientAddress + " sent ClientIdRequest");

            // start timeout and set latch to wait for the response
            startOperationTimeout(-1);
            sentOperationCompletedLatch = new CountDownLatch(1);
        } catch (IOException e) {
            System.out.println("[ERROR]: Failed to send ClientIdRequest for client " + clientAddress);
            throw new RuntimeException("Failed to send ClientIdRequest", e);
        }
    }

    private void performReadOperation(String queueName) {
        int operationId = nextOperationId.getAndIncrement();

        try {
            // ensure the outgoing socket exists
            ensureOutSocketExists();

            // send the request
            outStream.writeObject(new ReadRequest(queueName, clientId, operationId, clientAddress, brokerAddress));
            outStream.flush();

            System.out.println("[INFO]: Client " + clientId + " sent ReadRequest " + operationId);

            // start timeout and set latch to wait for the response
            startOperationTimeout(operationId);
            sentOperationCompletedLatch = new CountDownLatch(1);
        } catch (IOException e) {
            System.out.println("[ERROR]: Failed to send ReadRequest " + operationId + " for client " + clientId);
            failedOperations.add(operationId);
            outSocket = null;
            outStream = null;
        }
    }

    private void performWriteOperation(String queueName, int valueToWrite) {
        int operationId = nextOperationId.getAndIncrement();

        try {
            // ensure the outgoing socket exists
            ensureOutSocketExists();

            // send the request
            outStream.writeObject(new WriteRequest(queueName, valueToWrite, clientId, operationId, clientAddress, brokerAddress));
            outStream.flush();

            // store the written value locally
            if (checksStarted.get()) {
                writtenQueues.get(queueName).add(valueToWrite);
            }

            System.out.println("[INFO]: Client " + clientId + " sent WriteRequest " + operationId + " wiriting " + valueToWrite + " on queue " + queueName);

            // start timeout and set latch to wait for the response
            startOperationTimeout(operationId);
            sentOperationCompletedLatch = new CountDownLatch(1);
        } catch (IOException e) {
            System.out.println("[ERROR]: Failed to send WriteRequest " + operationId + " for client " + clientId);
            failedOperations.add(operationId);
            outSocket = null;
            outStream = null;
        }
    }

    private void sendAck(int operationId) {
        try {
            // ensure the outgoing socket exists
            ensureOutSocketExists();

            // send the ack
            outStream.writeObject(new RequestCompletedAck(clientAddress, operationId));
            outStream.flush();

            System.out.println("[INFO]: Client " + clientId + " sent Ack for operation " + operationId);
        } catch (IOException e) {
            System.out.println("[ERROR]: Failed to send Ack " + operationId + " for client " + clientId);
            outSocket = null;
            outStream = null;
        }
    }

    private void ensureOutSocketExists() {
        try {
            if (outSocket == null) {
                outSocket = new Socket();
                outSocket.setKeepAlive(true);
                outSocket.connect(new InetSocketAddress(brokerAddress.ip(), brokerAddress.port()), SOCKET_TIMEOUT_S * 1000);
                outStream = new ObjectOutputStream(outSocket.getOutputStream());
            }
        } catch (IOException e) {
            System.out.println("[ERROR]: Unable to create output socket: " + e.getMessage());
        }
    }

    private void startOperationTimeout(int operationId) {
        sentOperationTimeout = (new CompletableFuture<>())
                .orTimeout(OPERATION_TIMEOUT_S, TimeUnit.SECONDS)
                .exceptionally(ex -> {
                    if (ex instanceof TimeoutException) {
                        onOperationTimeoutExpired(operationId);
                    }
                    return null;
                });
    }

    private void onOperationTimeoutExpired(int operationId) {
        System.out.println("[ERROR]: Operation " + operationId + " timed out for client " + clientId);

        // register the failed operation
        failedOperations.add(operationId);

        // cancel timeout and notify the latch
        sentOperationTimeout.complete(null);
        sentOperationCompletedLatch.countDown();
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("======== RELIABLE QUEUING SYSTEM: CLIENT LOAD SIMULATION ========");

        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter broker address (ip:port) [default: 127.0.0.1:5001]: ");
        String brokerInput = scanner.nextLine().trim();
        Address brokerAddress = brokerInput.isEmpty() ?
                new Address("127.0.0.1", 5001) :
                parseAddress(brokerInput);

        // if loopback address has been specified as broker address, replace it with the actual IP address of the machine
        if (brokerAddress.ip().equals("127.0.0.1")) {
            brokerAddress = new Address(obtainClientIp(), brokerAddress.port());
        }

        System.out.print("Enter total operations to perform [default: 100]: ");
        String totalOperationsInput = scanner.nextLine().trim();
        int totalOperations = totalOperationsInput.isEmpty() ?
                100 :
                Integer.parseInt(totalOperationsInput);

        System.out.print("Enter read/write ratio [default: 0.3]: ");
        String readWriteRatioInput = scanner.nextLine().trim();
        double readWriteRatio = readWriteRatioInput.isEmpty() ?
                0.3 :
                Double.parseDouble(readWriteRatioInput);

        System.out.print("Enter queue names (comma-separated) [default: queueA,queueB,queueC,queueD,queueE]: ");
        String queueNamesInput = scanner.nextLine().trim();
        String[] queueNames = queueNamesInput.isEmpty() ?
                new String[]{"queueA", "queueB", "queueC", "queueD", "queueE"} :
                queueNamesInput.split(",");

        ClientLoadSim clientLoadSim = new ClientLoadSim(
                new Address(obtainClientIp(), obtainAvailablePort()),
                brokerAddress,
                totalOperations,
                readWriteRatio,
                queueNames
        );

        try {
            LoadSimResults simResults = clientLoadSim.start();

            // print the results
            System.out.println("\n====== Simulation of Client " + simResults.clientId() + " completed ======");
            System.out.println("Test duration: " + simResults.totalTime() + " ms");
            System.out.println("Total operations issued: " + simResults.totalOperations());
            System.out.println("Operations completed: " + simResults.successfulOperations());
            System.out.println("Operations failed: " + simResults.failedOperations());
            System.out.println("Lost operations: " + (simResults.totalOperations() - simResults.successfulOperations() - simResults.failedOperations()));
            System.out.println("Throughput: " + simResults.throughput() + " ops/s");
        } catch (RuntimeException e) {
            System.out.println("[FATAL ERROR]: Simulation of client " + clientLoadSim.clientAddress + " failed to obtain a client ID.");
        }
    }

    private static String obtainClientIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "127.0.0.1";
        }
    }

    private static int obtainAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            System.out.println("[ERROR]: Unable to obtain available port. Defaulting to 6000.");
            return 6000;
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
