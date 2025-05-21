package it.polimi.ds.reliable_queuing_system.test;

import it.polimi.ds.reliable_queuing_system.messages.*;
import it.polimi.ds.reliable_queuing_system.utils.Address;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ClientLoadSim {
    private final Address clientAddress;
    private final Address brokerAddress;
    private Integer clientId;

    private final AtomicInteger nextOperationId;
    private CountDownLatch sentOperationCompletedLatch;
    private CompletableFuture<Object> sentOperationTimeout;

    private final ExecutorService incomingSocketsExecutor;
    private final ExecutorService incomingMessagesExecutor;
    private CountDownLatch incomingMessageListenerReadyLatch;
    private Socket outSocket;
    private ObjectOutputStream outStream;
    private final AtomicBoolean simCompleted;

    private final int totalOperations;
    private final double readWriteRatio;
    private final String[] queueIds;
    private final List<Integer> completedOperations;
    private final List<Integer> failedOperations;

    private final AtomicLong startTime;
    private final AtomicLong endTime;

    private final Map<String, List<Integer>> writtenQueues;
    private final Map<Integer, String> readQueueNames;
    private final Map<Integer, List<Integer>> expectedValues;
    private final Map<Integer, List<Integer>> readValues;
    private CountDownLatch readValuesObtainedLatch;

    private static final int SOCKET_TIMEOUT_S = 30;
    private static final int EXECUTOR_SHUTDOWN_TIMEOUT_S = 30;
    private static final int OPERATION_TIMEOUT_S = 60;
    private static final int MIN_VALUE_TO_WRITE = 0;
    private static final int MAX_VALUE_TO_WRITE = 999;

    public ClientLoadSim(
            Address clientAddress,
            Address brokerAddress,
            int totalOperations,
            double readWriteRatio,
            String[] queueIds
    ) {
        this.clientAddress = clientAddress;
        this.brokerAddress = brokerAddress;
        this.clientId = null;

        this.nextOperationId = new AtomicInteger(0);
        this.sentOperationCompletedLatch = null;
        this.sentOperationTimeout = null;

        this.incomingSocketsExecutor = Executors.newFixedThreadPool(100);
        this.incomingMessagesExecutor = Executors.newFixedThreadPool(100);
        this.incomingMessageListenerReadyLatch = null;
        this.outSocket = null;
        this.outStream = null;
        this.simCompleted = new AtomicBoolean(false);

        this.totalOperations = totalOperations;
        this.readWriteRatio = readWriteRatio;
        this.queueIds = queueIds;
        this.completedOperations = new CopyOnWriteArrayList<>();
        this.failedOperations = new CopyOnWriteArrayList<>();

        this.startTime = new AtomicLong();
        this.endTime = new AtomicLong();

        this.writtenQueues = new ConcurrentHashMap<>();
        for (String queueId : queueIds) {
            writtenQueues.put(queueId, new CopyOnWriteArrayList<>());
        }
        this.readQueueNames = new ConcurrentHashMap<>();
        this.expectedValues = new ConcurrentHashMap<>();
        this.readValues = new ConcurrentHashMap<>();
        this.readValuesObtainedLatch = null;
    }

    public void start() throws InterruptedException {
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

        for (int i = 0; i < totalOperations; i++) {
            // determine if the operation is a read or a write
            boolean isRead = shouldPerformRead();

            // determine the queue to use
            String queueId = selectQueue();

            // perform the determined operation
            if (isRead) {
                performReadOperation(queueId);
            } else {
                int valueToWrite = ThreadLocalRandom.current().nextInt(MIN_VALUE_TO_WRITE, MAX_VALUE_TO_WRITE + 1);
                performWriteOperation(queueId, valueToWrite);
            }

            // wait for the operation to complete
            sentOperationCompletedLatch.await();

            Thread.sleep(500);
        }

        // register ending time
        endTime.set(System.currentTimeMillis());

        // mark the simulation as completed
        simCompleted.set(true);

        // shutdown the executors
        incomingMessagesExecutor.shutdownNow();
        incomingSocketsExecutor.shutdownNow();

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

        // print the results
        System.out.println("\n====== Simulation of Client " + clientAddress + " (id = " + clientId + ") completed ======");
        System.out.println("Test duration: " + (endTime.get() - startTime.get()) + " ms");
        System.out.println("Total operations issued: " + totalOperations);
        System.out.println("Operations completed: " + completedOperations.size());
        System.out.println("Operations failed: " + failedOperations.size());
        System.out.println("Lost operations: " + (totalOperations - completedOperations.size() - failedOperations.size()));
        System.out.println("Throughput: " + (completedOperations.size() / ((endTime.get() - startTime.get()) / 1000.0)) + " ops/s");
    }

    /// A method that will randomly return true or false based on the `readWriteRatio` parameter.
    private boolean shouldPerformRead() {
        return ThreadLocalRandom.current().nextDouble() < readWriteRatio;
    }

    /// A method that will return a random queueId out of the ones specified in the `queueIds` parameter.
    private String selectQueue() {
        int index = ThreadLocalRandom.current().nextInt(queueIds.length);
        return queueIds[index];
    }

    private void incomingMessagesListener() {
        try(ServerSocket serverSocket = new ServerSocket(clientAddress.port())) {
            System.out.println("Load test client ready to receive messages at port: " + clientAddress.port());

            incomingMessageListenerReadyLatch.countDown();

            while (!simCompleted.get() && !serverSocket.isClosed()) {
                try {
                    Socket socket = serverSocket.accept();
                    socket.setSoTimeout(SOCKET_TIMEOUT_S * 1000);

                    incomingMessagesExecutor.execute(() -> handleIncomingConnection(socket));
                }
                catch (IOException e) {
                    System.out.println("[ERROR]: Unable to accept incoming socket connection: " + e.getMessage());
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
                            case ReadConfirmation msg   -> handleReadConfirmation(msg);
                            case WriteResponse msg      -> handleWriteResponse(msg);
                            default -> System.out.println("[INFO]: Non-client message received.");
                        }
                    });
                } catch (ClassNotFoundException | ClassCastException e) {
                    System.out.println("[INFO]: Unknown message received: " + e.getMessage());
                } catch (IOException ignored) {
                    if (!simCompleted.get()) {
                        System.out.println("[CANE]: Socket closed? Is it safe to ignore?");  //TODO: check and hopefully remove
                    }
                    break;
                }
            }
            inSocket.close();
        } catch (IOException e) {
            System.out.println("[ERROR]: Unable to create ObjectInputStream: " + e.getMessage());
        }
    }

    private void handleClientIdAssignment(ClientIdAssignment msg) {
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
        System.out.println("[INFO]: Client " + clientAddress + " received ReadResponse for operation " + msg.operationId());

        // store the read values
        readQueueNames.put(msg.operationId(), msg.queueName());
        expectedValues.put(msg.operationId(), new CopyOnWriteArrayList<>(writtenQueues.get(msg.queueName())));
        readValues.put(msg.operationId(), new CopyOnWriteArrayList<>(msg.values()));

        readValuesObtainedLatch.countDown();
    }

    private void handleReadConfirmation(ReadConfirmation msg) {
        System.out.println("[INFO]: Client " + clientAddress + " received ReadConfirmation for operation " + msg.operationId());

        // wait for ReadResponse to be received and processed
        try {
            readValuesObtainedLatch.await();
        } catch (InterruptedException e) {
            System.out.println("[GABBIANO]: Read confirmation interrupted for client " + clientId);
            return;
        }

        // check if the read values are coherent with the written ones
        boolean success = true;
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

        // send ack to the broker
        sendAck(msg.operationId());

        // cancel timeout aWnd notify the latch
        sentOperationTimeout.complete(null);
        sentOperationCompletedLatch.countDown();
    }

    private void handleWriteResponse(WriteResponse msg) {
        System.out.println("[INFO]: Client " + clientAddress + " received WriteResponse for operation " + msg.operationId());

        // register the completed operation
        completedOperations.add(msg.operationId());

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
            outStream.writeObject(new ClientIdRequest(clientAddress));
            outStream.flush();

            System.out.println("[INFO]: Client " + clientAddress + " sent ClientIdRequest");

            // start timeout and set latch to wait for the response
            startOperationTimeout(-1);
            sentOperationCompletedLatch = new CountDownLatch(1);
        } catch (IOException e) {
            System.out.println("[ERROR]: Failed to send ClientIdRequest for client " + clientAddress);
            System.exit(1);  //TODO: maybe should be something else since it will run inside a thread
        }
    }

    private void performReadOperation(String queueName) {
        int operationId = nextOperationId.getAndIncrement();

        try {
            // ensure the outgoing socket exists
            ensureOutSocketExists();

            // send the request
            outStream.writeObject(new ReadRequest(queueName, clientId, operationId, clientAddress));
            outStream.flush();

            System.out.println("[INFO]: Client " + clientId + " sent ReadRequest " + operationId);

            // start timeout and set latch to wait for the response
            startOperationTimeout(operationId);
            sentOperationCompletedLatch = new CountDownLatch(1);
            readValuesObtainedLatch = new CountDownLatch(1);
        } catch (IOException e) {
            System.out.println("[ERROR]: Failed to send ReadRequest " + operationId + " for client " + clientId);
            failedOperations.add(operationId);
        }
    }

    private void performWriteOperation(String queueName, int valueToWrite) {
        int operationId = nextOperationId.getAndIncrement();

        try {
            // ensure the outgoing socket exists
            ensureOutSocketExists();

            // send the request
            outStream.writeObject(new WriteRequest(queueName, valueToWrite, clientId, operationId, clientAddress));
            outStream.flush();

            // store the written value locally
            writtenQueues.get(queueName).add(valueToWrite);

            System.out.println("[INFO]: Client " + clientId + " sent WriteRequest " + operationId);

            // start timeout and set latch to wait for the response
            startOperationTimeout(operationId);
            sentOperationCompletedLatch = new CountDownLatch(1);
        } catch (IOException e) {
            System.out.println("[ERROR]: Failed to send WriteRequest " + operationId + " for client " + clientId);
            failedOperations.add(operationId);
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
        }
    }

    private void ensureOutSocketExists() throws IOException {
        if (outSocket == null) {
            outSocket = new Socket();
            outSocket.connect(new InetSocketAddress(brokerAddress.ip(), brokerAddress.port()), SOCKET_TIMEOUT_S * 1000);
            outStream = new ObjectOutputStream(outSocket.getOutputStream());
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
        System.out.println("Hello people");

        ClientLoadSim clientLoadSim = new ClientLoadSim(
                new Address(obtainClientIp(), 6000),
                new Address("127.0.0.1", 5001),
                1000,
                0.4,
                new String[]{"queueA", "queueB", "queueC", "queueD", "queueE"}
        );

        clientLoadSim.start();
    }

    private static String obtainClientIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "127.0.0.1";
        }
    }
}
