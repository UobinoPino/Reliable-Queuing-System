package it.polimi.ds.reliable_queuing_system.client;

import it.polimi.ds.reliable_queuing_system.broker.NetworkManager;
import it.polimi.ds.reliable_queuing_system.messages.*;
import it.polimi.ds.reliable_queuing_system.utils.Address;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class Client {
    private static final Scanner scanner = new Scanner(System.in);

    private static Address clientAddress;

    private static Address brokerAddress;

    private static Socket currentSocket;
    private static ObjectOutputStream currentOutStream;

    private static Integer clientId;
    private static Integer nextOperationId = 0;

    private static final Lock clientStateLock = new ReentrantLock();
    private static ClientState clientState = ClientState.READY;

    private static final CountDownLatch isIncomingMessagesListenerReady = new CountDownLatch(1);

    private static final ScheduledExecutorService waitingTimeoutExecutor = Executors.newSingleThreadScheduledExecutor();
    private static ScheduledFuture<?> waitingTimeoutFuture;
    private static final AtomicInteger waitingTimeoutFailed = new AtomicInteger(0);
    private static final int WAITING_TIMEOUT_S = 60;

    // pool for handling connections in parallel
    private static final ExecutorService connectionPool = Executors.newFixedThreadPool(100);

    private static void startWaitingTimeout(Message sentMessage) {
        waitingTimeoutFuture = waitingTimeoutExecutor.schedule(() -> waitingTimeoutExpired(sentMessage), WAITING_TIMEOUT_S, TimeUnit.SECONDS);
    }

    private static void cancelWaitingTimeout() {
        if (waitingTimeoutFuture != null && !waitingTimeoutFuture.isDone()) {
            waitingTimeoutFuture.cancel(true);
            waitingTimeoutFailed.set(0);
        }
    }

    private static void waitingTimeoutExpired(Message sentMessage) {
        synchronized (clientStateLock) {
            if (clientState != ClientState.READY) {
                if (waitingTimeoutFailed.getAndIncrement() < 3) {
                    System.out.println("[ERROR]: Timeout expired while waiting for a response from the broker. Trying again...");

                    try {
                        currentOutStream.writeObject(sentMessage);
                        currentOutStream.flush();
                    } catch (IOException | NullPointerException ignored) {
                        System.out.println("[ERROR]: Could not connect to the known broker. Please specify a valid broker address.");
                        clientState = ClientState.READY;
                        cancelWaitingTimeout();
                        obtainKnownBrokerAddress();
                    }
                }
                else {
                    System.out.println("[ERROR]: Unable to receive response from the brokers. Please try specifying another known broker address.");
                    clientState = ClientState.READY;
                    cancelWaitingTimeout();
                    obtainKnownBrokerAddress();
                }
            }
        }
    }

    public static void main(String[] args) throws InterruptedException {
        System.out.println("======== RELIABLE QUEUING SYSTEM: CLIENT ========");

        // ask user to choose port
        String clientIp = obtainClientIp();
        int clientPort = obtainClientPort();
        clientAddress = new Address(clientIp, clientPort);

        // start thread to wait for incoming messages
        Thread messagesIngressThread = new Thread(Client::incomingMessagesListener);
        messagesIngressThread.start();
        isIncomingMessagesListenerReady.await();

        // request client id
        requestClientId();

        // enter TUI loop
        String input = "";
        do {
            synchronized (clientStateLock) {
                if (clientState == ClientState.READY) {
                    System.out.println("\nWhat do you want to do?");
                    System.out.println("\tr) Read the new values from a queue");
                    System.out.println("\tw) Write a new value in a queue");
                    System.out.println("\tq) Quit the application");
                    input = scanner.nextLine();

                    switch (input.toLowerCase()) {
                        case "r" -> performRead();
                        case "w" -> performWrite();
                        case "q" -> System.exit(0);
                        default -> System.out.println("[ERROR]: Invalid command. Please enter either 'r', 'w' or 'q'");
                    }
                }
            }
        } while (!input.equalsIgnoreCase("q"));
    }


    //region UTILITY FUNCTIONS

    private static String obtainClientIp() {
        try {
            return java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (java.net.UnknownHostException e) {
            return "127.0.0.1"; // Default to localhost if unable to determine
        }
    }

    private static Integer obtainClientPort() {
        Integer port = null;
        while (port == null) {
            System.out.print("Enter the port the client should listen to: ");
            try {
                int input = scanner.nextInt();
                if (input < 1024 || input > 65535) {
                    System.out.println("[ERROR]: Invalid port number. Please choose a number between 1024 and 65535.");
                } else {
                    port = input;
                }
            } catch (InputMismatchException e) {
                System.out.println("[ERROR]: Invalid port number. Please choose a number between 1024 and 65535.");
            }
            scanner.nextLine();
        }

        return port;
    }

    private static Address obtainKnownBrokerAddress() {
        String[] addr = null;
        while (addr == null) {
            System.out.print("Enter the address of a known broker (<ip>:<port>): ");
            String input = scanner.nextLine();
            String[] parts = input.split(":");
            if (parts.length != 2) {
                System.out.println("[ERROR]: Invalid address. Please specify a valid IP address with the format <ip>:<port>.");
            }
            else {
                try {
                    int port = Integer.parseInt(parts[1]);
                    if (port < 1024 || port > 65535) {
                        System.out.println("[ERROR]: Invalid port number. Please choose a number between 1024 and 65535.");
                    }
                    else {
                        addr = parts;
                    }
                } catch (NumberFormatException e) {
                    System.out.println("[ERROR]: Invalid port number. Please choose a number between 1024 and 65535.");
                }
            }
        }

        return new Address(addr[0], Integer.parseInt(addr[1]));
    }

    //endregion


    //region MESSAGE HANDLING FUNCTIONS

    private static void incomingMessagesListener() {
        try(ServerSocket serverSocket = new ServerSocket(clientAddress.port())) {
            System.out.println("Client ready to receive messages from brokers at port: " + clientAddress.port());
            isIncomingMessagesListenerReady.countDown();

            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                connectionPool.submit(() -> handleConnection(socket));
            }
        } catch (IOException e) {
            System.out.println("[FATAL ERROR]: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void handleConnection(Socket s) {
        try (Socket socket = s;
             ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {

            while (!socket.isClosed()){
                Message message = (Message) in.readObject();

                switch (message) {
                    case ClientIdAssignment msg -> handleClientIdAssignment(msg);
                    case ReadResponse msg -> handleReadResponse(msg);
                    case WriteResponse msg -> handleWriteResponse(msg);
                    default -> throw new ClassNotFoundException();
                }
            }
        }
        catch (ClassNotFoundException | ClassCastException ignored) {
            System.out.println("[INFO]: Unknown message received, it will be ignored.");
        }
        catch (IOException e) {
            System.out.println("[FATAL ERROR]: " + e.getMessage());
            System.exit(1);
        }
    }

    private static void handleClientIdAssignment(ClientIdAssignment msg) {
        synchronized (clientStateLock) {
            if (clientState == ClientState.WAITING_ID) {
                System.out.println("[INFO]: Obtained new client ID: " + msg.clientId());

                clientId = msg.clientId();
                clientState = ClientState.READY;
                cancelWaitingTimeout();

                try {
                    currentOutStream.writeObject(new RequestCompletedAck(clientAddress, -1));
                    currentOutStream.flush();
                } catch (IOException | NullPointerException ignored) {
                    // peer crashed but client already received the ID, so no need to do anything
                }
            } else {
                System.out.println("[INFO]: Unexpected ClientIdAssignment message received.");
            }
        }
    }

    private static void handleReadResponse(ReadResponse msg) {
        synchronized (clientStateLock) {
            if (clientState == ClientState.WAITING_READ) {
                if (msg.values().isEmpty()) {
                    System.out.println("No new values has been added to queue since last reading.");
                } else {
                    System.out.println("New values in queue have been found: " + msg.values());
                }
                clientState = ClientState.READY;
                cancelWaitingTimeout();

                try {
                    currentOutStream.writeObject(new RequestCompletedAck(clientAddress, msg.operationId()));
                    currentOutStream.flush();
                } catch (IOException | NullPointerException ignored) {
                    // peer crashed but client already received the read response, so no need to do anything
                }
            } else {
                System.out.println("[INFO]: Unexpected ReadResponse message received.");
            }
        }
    }

    private static void handleWriteResponse(WriteResponse msg) {
        synchronized (clientStateLock) {
            if (clientState == ClientState.WAITING_WRITE) {
                System.out.println("New value appended to the queue successfully.");

                clientState = ClientState.READY;
                cancelWaitingTimeout();

                try {
                    currentOutStream.writeObject(new RequestCompletedAck(clientAddress, msg.operationId()));
                    currentOutStream.flush();
                } catch (IOException | NullPointerException ignored) {
                    // peer crashed but client already received the write response, so no need to do anything
                }
            } else {
                System.out.println("[INFO]: Unexpected WriteResponse message received.");
            }
        }
    }

    //endregion


    //region USER ACTIONS FUNCTIONS

    private static void obtainOutSocket() {
        brokerAddress = obtainKnownBrokerAddress();

        try {
            currentSocket = new Socket();
            SocketAddress socketAddress = new java.net.InetSocketAddress(brokerAddress.ip(), brokerAddress.port());
            currentSocket.connect(socketAddress, NetworkManager.SOCKET_TIMEOUT);
            currentOutStream = new ObjectOutputStream(currentSocket.getOutputStream());
        } catch (IOException e) {
            System.out.println("[ERROR]: Unable to reach the broker at the given address. Please specify another one and try again.");
            obtainOutSocket();
        }
    }

    private static void requestClientId() {
        boolean requestSent = false;
        while (!requestSent) {
            synchronized (clientStateLock) {
                if (currentOutStream == null) {
                    obtainOutSocket();
                }

                try {
                    Message sentMessage = new ClientIdRequest(clientAddress, brokerAddress);
                    currentOutStream.writeObject(sentMessage);
                    currentOutStream.flush();
                    clientState = ClientState.WAITING_ID;
                    startWaitingTimeout(sentMessage);
                    requestSent = true;

                    System.out.println("[INFO]: Sent client ID request. If this is a reconnection, you'll receive your previous ID.");
                } catch (IOException e) {
                    System.out.println("[ERROR]: Could not connect to the broker. Please specify a valid broker address.");
                    obtainOutSocket();
                }
            }
        }
    }

    private static void performRead() {
        // collect the id of the queue to read
        System.out.print("Please insert the id of the queue you want to read from: ");
        String queueId = scanner.nextLine();

        synchronized (clientStateLock) {
            if (currentOutStream == null) {
                obtainOutSocket();
            }

            try {
                Message sentMessage = new ReadRequest(queueId, clientId, nextOperationId++, clientAddress, brokerAddress);
                currentOutStream.writeObject(sentMessage);
                currentOutStream.flush();

                clientState = ClientState.WAITING_READ;
                startWaitingTimeout(sentMessage);
            } catch (IOException e) {
                System.out.println("[ERROR]: Unable to reach the broker at the given address. Please specify another one and try again.");
                obtainOutSocket();
            }
        }
    }

    private static void performWrite() {
        // collect the id of the queue to write on
        System.out.print("Please insert the id of the queue you want to write on: ");
        String queueId = scanner.nextLine();

        // obtain the value to be added to the queue
        Integer newValue = null;
        while (newValue == null) {
            System.out.print("Please insert the new value you want to append to the queue: ");
            try {
                newValue = scanner.nextInt();
                scanner.nextLine(); // consume the remaining newline
            } catch (InputMismatchException e) {
                System.out.println("[ERROR]: Invalid input. Please enter a valid integer.");
                scanner.nextLine(); // clear the scanner buffer
            }
        }

        synchronized (clientStateLock) {
            if (currentOutStream == null) {
                obtainOutSocket();
            }

            try {
                Message sentMessage = new WriteRequest(queueId, newValue, clientId, nextOperationId++, clientAddress, brokerAddress);
                currentOutStream.writeObject(sentMessage);
                currentOutStream.flush();

                clientState = ClientState.WAITING_WRITE;
                startWaitingTimeout(sentMessage);
            } catch (IOException e) {
                System.out.println("[ERROR]: Unable to reach the broker at the given address. Please specify another one and try again.");
                obtainOutSocket();
            }
        }
    }

    //endregion
}
