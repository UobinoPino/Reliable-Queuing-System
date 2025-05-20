package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.*;
import it.polimi.ds.reliable_queuing_system.utils.Address;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.*;
import java.util.Arrays;
import java.util.InputMismatchException;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Broker entry point. Uses a fixed thread‐pool to handle each incoming socket.
 * Each connection is read in a loop (readObject until EOF) so pooled sockets
 * (used by NetworkManager) can carry multiple messages (heartbeats, dispatch calls, ...).
 */
public class Broker {
    private static final Scanner scanner = new Scanner(System.in);

    private static Address brokerAddress;
    private static SharedState sharedState;
    private static Integer brokerId;
    private static NetworkManager networkManager;
    private static HeartbeatManager heartbeatManager;
    private static LogManager logManager;
    private static MessageDispatcher messageDispatcher;

    private static final Lock brokerStateLock = new ReentrantLock();
    private static BrokerState brokerState;
    public static final ElectionInfo electionInfo = new ElectionInfo();

    // pool for handling connections in parallel
    private static final ExecutorService connectionPool = Executors.newCachedThreadPool();
    private static final ExecutorService dispatchPool   = Executors.newCachedThreadPool();

    private static void joinCluster() {
        boolean requestSent = false;
        while (!requestSent) {
            synchronized (brokerStateLock) {
                Address known = obtainKnownBrokerAddress();
                try (Socket sock = new Socket()) {
                    sock.connect(new InetSocketAddress(known.ip(), known.port()), NetworkManager.SOCKET_TIMEOUT);
                    ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
                    out.writeObject(new BrokerJoinRequest(brokerAddress));
                    out.flush();

                    brokerState = BrokerState.WAITING_JOIN;
                    requestSent = true;
                    System.out.println("[INFO]: Sent join request to " + known);
                } catch (IOException e) {
                    System.out.println("[ERROR]: Could not connect to broker at " + known + ". Try again.");
                }
            }
        }
    }
    public static void main(String[] args) {
        System.out.println("======== RELIABLE QUEUING SYSTEM: BROKER ========");

        // ask user to choose port
        String brokerIp = obtainBrokerIp();
        Integer brokerPort = obtainBrokerPort();
        brokerAddress = new Address(brokerIp, brokerPort);

        try (ServerSocket serverSocket = new ServerSocket(brokerAddress.port())) {
            // first broker vs join logic unchanged
            if (Arrays.asList(args).contains("--first")) {
                synchronized (brokerStateLock) {
                    sharedState = new SharedState();
                    brokerId = sharedState.getNewBrokerId();
                    sharedState.addBrokerAddress(brokerId, brokerAddress);
                    System.out.println("[INFO]: First broker " + brokerId + " on " + brokerAddress);
                    sharedState.setNewLeaderId(brokerId);
                    initializeManagers();
                    brokerState = BrokerState.READY;
                }
            } else {
                joinCluster();
            }

            // accept loop – submit each socket to connectionPool
            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                connectionPool.submit(() -> handleConnection(socket));
            }
        } catch (IOException e) {
            System.out.println("[FATAL ERROR] " + e.getMessage());
            System.exit(1);
        } finally {
            shutdown();
        }
    }

    private static void handleConnection(Socket socket) {
        try (Socket s = socket;
             ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {

            while (!socket.isClosed()) {
                Message msg = (Message) in.readObject();

                synchronized (brokerStateLock) {
                    if (brokerState == BrokerState.WAITING_JOIN) {
                        if (msg instanceof BrokerJoinResponse resp) {
                            brokerId = resp.newBrokerId();
                            sharedState = resp.sharedState();
                            sharedState.persistState();
                            brokerState = BrokerState.READY;
                            initializeManagers();
                            System.out.println("[INFO]: Joined cluster as broker " + brokerId);
                        } else {
                            System.out.println("[INFO]: Ignoring pre-join message: " + msg);
                        }
                    } else {
                        dispatchPool.submit(() -> {
                            try {
                                messageDispatcher.dispatch(msg);
                            } catch (Exception e) {
                                System.err.println("[ERROR]: dispatch failed: " + e.getMessage());
                            }
                        });
                    }
                }
            }
        } catch (ClassNotFoundException | ClassCastException e) {
            System.out.println("[INFO]: Unknown message received, it will be ignored.");
        } catch (IOException ignored) {
            // connection closed by peer. no need to do anything
        }
    }

    private static void shutdown() {
        connectionPool.shutdown();
        try {
            if (!connectionPool.awaitTermination(5, TimeUnit.SECONDS)) {
                connectionPool.shutdownNow();
                System.out.println("[INFO]: Connection pool shutdown timed out. Forcefully shutting down.");
            }
        } catch (InterruptedException ignored) {
            connectionPool.shutdownNow();
        }
        dispatchPool.shutdown();
        try {
            if (!dispatchPool.awaitTermination(5, TimeUnit.SECONDS)) {
                dispatchPool.shutdownNow();
                System.out.println("[INFO]: Dispatch pool shutdown timed out. Forcefully shutting down.");
            }
        } catch (InterruptedException ignored) {
            dispatchPool.shutdownNow();
        }
        networkManager.shutdown(); // clean up pooled sockets
    }

        /// Returns the ip of the node this class is executed on
    /// (or localhost ip if unable to determine it).
    private static String obtainBrokerIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "127.0.0.1"; // Default to localhost if unable to determine
        }
    }

    /// Requests the user to input a valid port and returns it.
    private static Integer obtainBrokerPort() {
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

    /// Requests the user to input a valid address for a known broker in the format \<ip\>:\<port\>
    /// and returns it.
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

    /// Create an instance for each one of the Manager classes used by the Broker.
    private static void initializeManagers() {
        networkManager = new NetworkManager(electionInfo);
        logManager = new LogManager(brokerId, sharedState, networkManager);
        heartbeatManager = new HeartbeatManager(brokerId, sharedState, electionInfo, networkManager,logManager);
        messageDispatcher = new MessageDispatcher(
                brokerId,
                sharedState,
                heartbeatManager,
                logManager,
                networkManager,
                electionInfo
        );
    }
}