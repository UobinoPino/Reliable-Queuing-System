package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.*;
import it.polimi.ds.reliable_queuing_system.utils.Address;
import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class Broker {
    private static final Scanner scanner = new Scanner(System.in);

    private static Address brokerAddress;

    private static SharedState sharedState;
    private static Integer brokerId;

    private static BrokerState brokerState;

    private static final long HEARTBEAT_INTERVAL_MS = 500; // 5 seconds
    private static final long HEARTBEAT_TIMEOUT_MS = 1500; // 15 seconds

    private static Thread heartbeatSenderThread;
    private static Thread heartbeatMonitorThread;

    public static void main(String[] args) {
        System.out.println("======== RELIABLE QUEUING SYSTEM: BROKER ========");

        // ask user to choose port
        String brokerIp = obtainBrokerIp();
        Integer brokerPort = obtainBrokerPort();
        brokerAddress = new Address(brokerIp, brokerPort);

        // obtain the correct sharedState and brokerId, depending on if this broker is the first broker of the system or not
        if (Arrays.asList(args).contains("--first")) {
            sharedState = new SharedState();
            brokerId = sharedState.getNewBrokerId();
            sharedState.addBrokerAddress(brokerId, brokerAddress);
            sharedState.setNewLeaderId(brokerId);

            // Start heartbeat mechanism for the leader
            startHeartbeatMechanism();
        }
        else {
            // ask the user to specify the address of a known broker
            Address knownBrokerAddress = obtainKnownBrokerAddress();

            // send the join request to the known broker
            try(Socket socket = new Socket(knownBrokerAddress.ip(), knownBrokerAddress.port())) {
                ObjectOutputStream toExistingBroker = new ObjectOutputStream(socket.getOutputStream());
                toExistingBroker.writeObject(new BrokerJoinRequest(brokerAddress));
                toExistingBroker.flush();

                brokerState = BrokerState.WAITING_JOIN;
            } catch (IOException e) {
                System.out.println("[FATAL ERROR]: " + e.getMessage());
                System.exit(1);
                //TODO: maybe we could re-ask another address instead of crashing
            }
        }

        try(ServerSocket serverSocket = new ServerSocket(brokerAddress.port())) {
            System.out.println("Broker ready to receive messages at port: " + brokerAddress.port());

            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                ObjectInputStream in  = new ObjectInputStream(socket.getInputStream());

                try {
                    Message message = (Message) in.readObject();

                    if (brokerState == BrokerState.WAITING_JOIN) {
                        if (message instanceof BrokerJoinResponse brokerJoinResponse) {
                            brokerId = brokerJoinResponse.newBrokerId();
                            sharedState = brokerJoinResponse.sharedState();
                            brokerState = BrokerState.READY;

                            // Start heartbeat mechanism
                            startHeartbeatMechanism();
                        }
                        else {
                            System.out.println("[INFO]: Received message before joining the cluster. It will be ignored.");
                        }
                    }
                    else {
                        switch (message) {
                            case EntryPropagation msg -> handleEntryPropagation(msg);
                            case EntryPropagationAck msg -> handleEntryPropagationAck(msg);
                            case EntryCommit msg -> handleEntryCommit(msg);
                            case BrokerJoinRequest msg -> handleBrokerJoinRequest(msg);
                            case ClientIdRequest msg -> handleClientIdRequest(msg);
                            case ReadRequest msg -> handleReadRequest(msg);
                            case ClientOffsetsUpdate msg -> handleClientOffsetsUpdate(msg);
                            case WriteRequest msg -> handleWriteRequest(msg);
                            case Heartbeat msg -> handleHeartbeat(msg);
                            case HeartbeatAck msg -> handleHeartbeatAck(msg);
                            //TODO: implement and uncomment the following:
//                            case BrokerRemoval msg -> handleBrokerRemoval(msg);
//                            case NewLeaderNomination msg -> handleNewLeaderNomination(msg);
//                            case NewLeaderNominationAck msg -> handleNewLeaderNominationAck(msg);
//                            case NewLeaderAnnouncement msg -> handleNewLeaderAnnouncement(msg);
                            default -> throw new ClassNotFoundException();
                        }
                    }
                } catch (ClassNotFoundException | ClassCastException ignored) {
                    System.out.println("[INFO]: Unknown message received, it will be ignored.");
                }
            }
        } catch(IOException e) {
            System.out.println("[FATAL ERROR] " + e.getMessage());
            System.exit(1);
        }
    }


    //region UTILITY FUNCTIONS

    /// Returns the ip of the node this class is executed on
    /// (or localhost ip if unable to determine it).
    private static String obtainBrokerIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "127.0.0.1"; // Default to localhost if unable to determine
        }
    }

    /// Requests the user to input a valid port by using the given prompt
    /// and returns it.
    private static Integer obtainBrokerPort() {
        Integer port = null;
        while (port == null) {
            System.out.print("Enter the port the client should listen to: ");
            try {
                int input = scanner.nextInt();
                scanner.nextLine();
                if (input < 1024 || input > 65535) {
                    System.out.println("[ERROR]: Invalid port number. Please choose a number between 1024 and 65535.");
                } else {
                    port = input;
                }
            } catch (InputMismatchException e) {
                System.out.println("[ERROR]: Invalid port number. Please choose a number between 1024 and 65535.");
            }
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

    /// Returns whether this broker is the current leader of the system or not
    private static boolean isLeader() {
        return sharedState.isLeader(brokerId);
    }

    /// Forwards the given message to the current system leader
    private static void forwardMessageToLeader(Message msg) {
        Address leaderAddr = sharedState.getBrokerAddress(sharedState.getLeaderId());

        try(Socket socket = new Socket(leaderAddr.ip(), leaderAddr.port())) {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(msg);
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
            //TODO: replace with proper error handling
            // (maybe it should trigger a new election?)
        }
    }

    /// Broadcasts the given [Message] to all other brokers in the system
    private static void broadcastMessage(Message message) {
        Map<Integer, Address> brokerAddresses = sharedState.getBrokerAddresses();

        for (Integer id : brokerAddresses.keySet()) {
            if (!id.equals(brokerId)) {
                Address addr = brokerAddresses.get(id);
                try(Socket socket = new Socket(addr.ip(), addr.port())) {
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(message);
                    out.flush();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                    //TODO: replace with proper error handling
                    // (maybe it should trigger node removal?)
                }
            }
        }
    }

    /// Send the given [Message] to the given [Address]
    private static void sendMessage(Address address, Message message) {
        try(Socket socket = new Socket(address.ip(), address.port())) {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(message);
            out.flush();
        } catch (IOException e) {
            System.out.println("Oh no, anyway");  //TODO: replace with proper error handling
        }
    }

    //endregion


    //region MESSAGE HANDLING FUNCTIONS

    private static void handleEntryPropagation(EntryPropagation msg) {
        if (!isLeader()) {
            // store the entry as waiting for commit
            sharedState.addWaitingCommitEntry(msg.logEntry());

            // send an ACK to the leader
            Address leaderAddr = sharedState.getBrokerAddress(sharedState.getLeaderId());
            sendMessage(leaderAddr, new EntryPropagationAck(msg.logEntry()));
        }
    }

    private static void handleEntryPropagationAck(EntryPropagationAck msg) {
        if (isLeader()) {
            if(sharedState.isEntryWaitingAck(msg.logEntry())) {
                // commit the entry locally
                sharedState.commitEntry(msg.logEntry());

                // perform the operation described in the log entry
                performEntryOperation(msg.logEntry());

                // broadcast the EntryCommit message
                broadcastMessage(new EntryCommit(msg.logEntry()));
            }
        }
    }

    private static void handleEntryCommit(EntryCommit msg) {
        if (!isLeader()) {
            // commit the entry locally
            sharedState.commitEntry(msg.logEntry());

            // perform the operation described in the log entry
            performEntryOperation(msg.logEntry());
        }
    }

    private static void handleBrokerJoinRequest(BrokerJoinRequest msg) {
        if (isLeader()) {
            // create a new log entry for the broker join
            LogEntry newEntry = new LogEntry(sharedState.getLogLength(), msg);

            // if multi-broker system, store the entry as waiting for ACK and propagate it
            if (sharedState.getBrokersCount() > 1) {
                sharedState.addWaitingAckEntry(newEntry);
                broadcastMessage(new EntryPropagation(newEntry));
            }
            // else (single-broker system) commit the entry and perform the id assignment operation
            else {
                sharedState.commitEntry(newEntry);
                addBroker(msg);
            }
        }
        else {
            forwardMessageToLeader(msg);
        }
    }

    private static void handleClientIdRequest(ClientIdRequest msg) {
        if (isLeader()) {
            // create a new log entry
            LogEntry newEntry = new LogEntry(sharedState.getLogLength(), msg);

            // if multi-broker system, store the entry as waiting for ACK and propagate it
            if (sharedState.getBrokersCount() > 1) {
                sharedState.addWaitingAckEntry(newEntry);
                broadcastMessage(new EntryPropagation(newEntry));
            }
            // else (single-broker system) commit the entry and perform the id assignment operation
            else {
                sharedState.commitEntry(newEntry);
                assignClientId(msg);
            }
        }
        else {
            forwardMessageToLeader(msg);
        }
    }

    private static void handleReadRequest(ReadRequest msg) {
        // retrieve the values to be returned to the client
        List<Integer> valuesToReturn = new ArrayList<>();
        List<Integer> requestedQueue = sharedState.getQueue(msg.queueName());
        if(!requestedQueue.isEmpty()) {
            int clientOffset = sharedState.getClientOffset(msg.clientId(), msg.queueName());
            if (clientOffset < requestedQueue.size()) {
                valuesToReturn.addAll(requestedQueue.subList(clientOffset, requestedQueue.size()));
            }
        }

        // send the ReadResponse to the client
        sendMessage(msg.clientAddress(), new ReadResponse(msg.clientId(), msg.operationId(), valuesToReturn));

        // create a new ClientOffsetUpdate message and handle it accordingly
        ClientOffsetsUpdate offsetsUpdateMsg = new ClientOffsetsUpdate(msg.queueName(), requestedQueue.size(), msg.clientId(), msg.operationId(), msg.clientAddress());
        handleClientOffsetsUpdate(offsetsUpdateMsg);
    }

    private static void handleClientOffsetsUpdate(ClientOffsetsUpdate msg) {
        if(isLeader()) {
            // create new log entry for the offset update
            LogEntry newEntry = new LogEntry(sharedState.getLogLength(), msg);

            // if multi-broker system, store the entry as waiting for ACK and propagate it
            if (sharedState.getBrokersCount() > 1) {
                sharedState.addWaitingAckEntry(newEntry);
                broadcastMessage(new EntryPropagation(newEntry));
            }
            // else (single-broker system) commit the entry and perform the offset update
            else {
                sharedState.commitEntry(newEntry);
                updateClientOffset(msg);
            }
        }
        else {
            forwardMessageToLeader(msg);
        }
    }

    private static void handleWriteRequest(WriteRequest msg) {
        if(isLeader()) {
            // create new log entry for the write operation
            LogEntry newEntry = new LogEntry(sharedState.getLogLength(), msg);

            // if multi-broker system store entry as waiting for ACK and propagate it
            if(sharedState.getBrokersCount() > 1) {
                sharedState.addWaitingAckEntry(newEntry);
                broadcastMessage(new EntryPropagation(newEntry));
            }
            // else (single-broker system) commit it and perform the write
            else {
                sharedState.commitEntry(newEntry);
                writeValue(msg);
            }
        }
        else {
            forwardMessageToLeader(msg);
        }
    }

    private static void handleHeartbeat(Heartbeat heartbeat) {
        System.out.println("[INFO]: Received heartbeat from leader");

        // Update timestamp in shared state
        sharedState.updateLastHeartbeatReceived(brokerId);

        try {
            // Identify which broker sent the heartbeat
            Address leaderAddr = sharedState.getBrokerAddress(sharedState.getLeaderId());
            sendMessage(leaderAddr, new HeartbeatAck(brokerId));  // Include our broker ID in the ack
        } catch (Exception e) {
            System.out.println("[ERROR]: Failed to send heartbeat acknowledgment: " + e.getMessage());
        }
    }

    private static void handleHeartbeatAck(HeartbeatAck heartbeatAck) {
        if (isLeader()) {
            Integer followerId = heartbeatAck.brokerId();
            if (followerId != null) {
                sharedState.updateLastHeartbeatAckReceived(followerId);
                System.out.println("[INFO]: Received heartbeat acknowledgment from broker " + followerId);
            } else {
                System.out.println("[WARNING]: Received heartbeat acknowledgment without broker ID");
            }
        }
    }

    private static void startHeartbeatMechanism() {
        stopHeartbeatThreads();

        if (isLeader()) {
            startHeartbeatSender();
        } else {
            startHeartbeatMonitor();
        }
    }

    private static void stopHeartbeatThreads() {
        if (heartbeatSenderThread != null) {
            heartbeatSenderThread.interrupt();
        }
        if (heartbeatMonitorThread != null) {
            heartbeatMonitorThread.interrupt();
        }
    }

    private static void startHeartbeatSender() {
        heartbeatSenderThread = new Thread(() -> {
            try {
                while (!Thread.interrupted()) {
                    // Send heartbeat to all followers
                    Map<Integer, Address> brokerAddresses = sharedState.getBrokerAddresses();
                    for (Integer id : brokerAddresses.keySet()) {
                        if (!id.equals(brokerId)) {
                            try {
                                sendMessage(brokerAddresses.get(id), new Heartbeat());
                            } catch (Exception e) {
                                System.out.println("[WARNING]: Failed to send heartbeat to broker " + id);
                            }
                        }
                    }

                    // Check for follower timeouts
                    sharedState.checkFollowerTimeouts(HEARTBEAT_TIMEOUT_MS);

                    Thread.sleep(HEARTBEAT_INTERVAL_MS);
                }
            } catch (InterruptedException e) {
                // Exit gracefully
            }
        });
        heartbeatSenderThread.setDaemon(true);
        heartbeatSenderThread.start();
    }

    private static void startHeartbeatMonitor() {
        heartbeatMonitorThread = new Thread(() -> {
            try {
                while (!Thread.interrupted()) {
                    // Check if leader has timed out
                    sharedState.checkLeaderTimeout(brokerId, HEARTBEAT_TIMEOUT_MS);

                    Thread.sleep(HEARTBEAT_TIMEOUT_MS / 3);
                }
            } catch (InterruptedException e) {
                // Exit gracefully
            }
        });
        heartbeatMonitorThread.setDaemon(true);
        heartbeatMonitorThread.start();
    }

    //endregion


    //region ENTRY OPERATIONS

    private static void performEntryOperation(LogEntry entry) {
        Message msg = entry.message();
        switch (msg) {
            case BrokerJoinRequest m -> addBroker(m);
            case ClientIdRequest m -> assignClientId(m);
            case ClientOffsetsUpdate m -> updateClientOffset(m);
            case WriteRequest m -> writeValue(m);
            //TODO: add all the other actions that should be performed by the leader
            default -> throw new IllegalStateException("Unexpected value: " + msg);  //TODO: replace with proper error handling
        }
    }

    private static void addBroker(BrokerJoinRequest req) {
        // get a new broker id from the shared state
        int newBrokerId = sharedState.getNewBrokerId();

        // add the broker to the list in the shared state
        sharedState.addBrokerAddress(newBrokerId, req.brokerAddress());

        // if leader, return a BrokerJoinResponse to the requesting broker
        if (isLeader()) {
            sendMessage(req.brokerAddress(), new BrokerJoinResponse(newBrokerId, sharedState));
        }
    }

    private static void assignClientId(ClientIdRequest req) {
        // get a new client id from the shared state
        int clientId = sharedState.getNewClientId();

        // if leader, return this id to the requesting client
        if (isLeader()) {
            sendMessage(req.clientAddress(), new ClientIdAssignment(clientId));
        }
    }

    private static void updateClientOffset(ClientOffsetsUpdate req) {
        // update the offsets
        sharedState.updateClientOffset(req.clientId(), req.queueName(), req.newOffset());

        // if leader, return the read confirmation to the requesting client
        if(isLeader()){
            sendMessage(req.clientAddress(), new ReadConfirmation(req.operationId()));
        }
    }

    private static void writeValue(WriteRequest req) {
        // write the new value in the queue
        sharedState.addToQueue(req.queueName(), req.value());

        // if leader, send the WriteResponse to the requesting client
        if(isLeader()){
            sendMessage(req.clientAddress(), new WriteResponse(req.operationId()));
        }
    }

    //endregion
}