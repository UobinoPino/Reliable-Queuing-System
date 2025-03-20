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

    private static final long HEARTBEAT_INTERVAL_MS = 500;
    private static final long HEARTBEAT_TIMEOUT_MS = 2000;

    private static Thread heartbeatSenderThread;
    private static Thread heartbeatMonitorThread;

    // Election-related variables
    private static volatile boolean electionInProgress = false;
    private static volatile int currentBestCandidate = -1;
    private static volatile int currentBestLogLength = -1;
    private static final Set<Integer> receivedAcks = Collections.synchronizedSet(new HashSet<>());
    private static final Object electionLock = new Object();
    private static final Random random = new Random();

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
            //TODO: print the brokerId and address
            System.out.println("First: Broker " + brokerId + " started with address: " + brokerAddress);
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
                            System.out.println("Not first: Broker " + brokerId + " started with address: " + brokerAddress);
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
                            case BrokerRemoval msg -> handleBrokerRemoval(msg);
                            case NewLeaderNomination msg -> handleNewLeaderNomination(msg);
                            case NewLeaderNominationAck msg -> handleNewLeaderNominationAck(msg);
                            case NewLeaderAnnouncement msg -> handleNewLeaderAnnouncement(msg);
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

            System.out.println("Failed to send message to " + address + ":  it has probably crashed.");



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
    private static void handleBrokerRemoval(BrokerRemoval msg) {
        int removedBrokerId = msg.brokerId();

        // Only process if we're not the leader (leader already removed it)
        if (!isLeader()) {
            System.out.println("Follower " + brokerId + ": Received broker removal notification for broker " + removedBrokerId);
            sharedState.removeBrokerAddress(removedBrokerId);
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

 /*  private static void startHeartbeatSender() {
       heartbeatSenderThread = new Thread(() -> {
           try {
               // Track brokers that we've detected as failed
               Set<Integer> knownFailedBrokers = new HashSet<>();

               while (!Thread.interrupted()) {
                   // Get up-to-date broker list each cycle
                   Map<Integer, Address> brokerAddresses = new HashMap<>(sharedState.getBrokerAddresses());

                   // Remove any previously failed brokers from our tracking set if they're no longer in brokerAddresses
                   knownFailedBrokers.removeIf(id -> !brokerAddresses.containsKey(id));

                   // Send heartbeat to all active followers (skip failed ones)
                   for (Integer id : brokerAddresses.keySet()) {
                       if (!id.equals(brokerId) && !knownFailedBrokers.contains(id)) {
                           try {
                               System.out.println("Leader: Sending heartbeat to broker " + id);
                               sendMessage(brokerAddresses.get(id), new Heartbeat());
                           } catch (Exception e) {
                               System.out.println("[WARNING]: Failed to send heartbeat to broker " + id);
                           }
                       }
                   }

                   // Check for follower timeouts
                   List<Integer> removedBrokers = sharedState.checkFollowerTimeouts(HEARTBEAT_TIMEOUT_MS);

                   // If any brokers were removed, add them to failed set and notify others
                   if (!removedBrokers.isEmpty()) {
                       System.out.println("Leader: Detected removed brokers: " + removedBrokers);
                       knownFailedBrokers.addAll(removedBrokers);

                       // Get remaining brokers after removal
                       Map<Integer, Address> remainingBrokers = sharedState.getBrokerAddresses();

                       // Broadcast removal notifications
                       for (Integer removedBrokerId : removedBrokers) {
                           System.out.println("Leader: Broadcasting removal of broker " + removedBrokerId);

                           for (Map.Entry<Integer, Address> broker : remainingBrokers.entrySet()) {
                               Integer targetId = broker.getKey();
                               if (!targetId.equals(brokerId)) {
                                   try {
                                       sendMessage(broker.getValue(), new BrokerRemoval(removedBrokerId));
                                   } catch (Exception e) {
                                       System.out.println("[WARNING]: Failed to notify broker " + targetId +
                                               " about removal of broker " + removedBrokerId);
                                   }
                               }
                           }
                       }
                   }

                   Thread.sleep(HEARTBEAT_INTERVAL_MS);
               }
           } catch (InterruptedException e) {
               // Exit gracefully
           }
       });
       heartbeatSenderThread.setDaemon(true);
       heartbeatSenderThread.start();
   } */
/* private static void startHeartbeatSender() {
     heartbeatSenderThread = new Thread(() -> {
         try {
             System.out.println("Leader " + brokerId + ": Starting heartbeat sender thread");
             // Track brokers that we've detected as failed
             Set<Integer> knownFailedBrokers = new HashSet<>();

             while (!Thread.interrupted()) {
                 // Get up-to-date broker list each cycle
                 Map<Integer, Address> brokerAddresses = new HashMap<>(sharedState.getBrokerAddresses());

                 // Remove any previously failed brokers from our tracking set if they're no longer in brokerAddresses
                 knownFailedBrokers.removeIf(id -> !brokerAddresses.containsKey(id));

                 // Send heartbeat to all active followers (skip failed ones)
                 for (Integer id : brokerAddresses.keySet()) {
                     if (!id.equals(brokerId) && !knownFailedBrokers.contains(id)) {
                         try {
                             Address followerAddr = brokerAddresses.get(id);
                             System.out.println("Leader " + brokerId + ": Sending heartbeat to broker " + id + " at " + followerAddr);
                             Socket socket = new Socket(followerAddr.ip(), followerAddr.port());
                             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                             out.writeObject(new Heartbeat());
                             out.flush();
                             socket.close();
                         } catch (Exception e) {
                             System.out.println("[WARNING]: Failed to send heartbeat to broker " + id + ": " + e.getMessage());
                         }
                     }
                 }

                 // Check for follower timeouts
                 List<Integer> removedBrokers = sharedState.checkFollowerTimeouts(HEARTBEAT_TIMEOUT_MS);

                 // If any brokers were removed, add them to failed set and notify others
                 if (!removedBrokers.isEmpty()) {
                     System.out.println("Leader " + brokerId + ": Detected removed brokers: " + removedBrokers);
                     knownFailedBrokers.addAll(removedBrokers);

                     // Get remaining brokers after removal
                     Map<Integer, Address> remainingBrokers = sharedState.getBrokerAddresses();

                     // Broadcast removal notifications
                     for (Integer removedBrokerId : removedBrokers) {
                         System.out.println("Leader " + brokerId + ": Broadcasting removal of broker " + removedBrokerId);

                         for (Map.Entry<Integer, Address> broker : remainingBrokers.entrySet()) {
                             Integer targetId = broker.getKey();
                             if (!targetId.equals(brokerId)) {
                                 try {
                                     sendMessage(broker.getValue(), new BrokerRemoval(removedBrokerId));
                                 } catch (Exception e) {
                                     System.out.println("[WARNING]: Failed to notify broker " + targetId +
                                             " about removal of broker " + removedBrokerId);
                                 }
                             }
                         }
                     }
                 }

                 Thread.sleep(HEARTBEAT_INTERVAL_MS);
             }
         } catch (InterruptedException e) {
             // Exit gracefully
             System.out.println("Leader " + brokerId + ": Heartbeat sender thread interrupted");
         } catch (Exception e) {
             System.out.println("Leader " + brokerId + ": Error in heartbeat sender: " + e.getMessage());
         }
     });
     heartbeatSenderThread.setDaemon(true);
     heartbeatSenderThread.start();
     System.out.println("Leader " + brokerId + ": Heartbeat sender thread started");
 } */
 private static void startHeartbeatSender() {
     heartbeatSenderThread = new Thread(() -> {
         try {
             System.out.println("Leader " + brokerId + ": Starting heartbeat sender thread");
             Set<Integer> knownFailedBrokers = new HashSet<>();

             // Get my own address once for comparison
             Address myAddress = sharedState.getBrokerAddress(brokerId);
             if (myAddress == null) {
                 System.out.println("[ERROR]: Cannot find my own address in broker list");
                 return;
             }

             while (!Thread.interrupted()) {
                 Map<Integer, Address> brokerAddresses = sharedState.getBrokerAddresses();
                 System.out.println("startHeartbeatSender: Broker " + brokerId + ": Current brokers: " + sharedState.getBrokerAddresses());
                 knownFailedBrokers.removeIf(id -> !brokerAddresses.containsKey(id));

                 // Send heartbeat to all active followers, avoiding same-address brokers
                 for (Integer id : brokerAddresses.keySet()) {
                     Address followerAddr = brokerAddresses.get(id);

                     // Skip myself by ID and any broker with identical address
                     if (id.equals(brokerId) ||
                             (followerAddr.ip().equals(myAddress.ip()) &&
                                     Objects.equals(followerAddr.port(), myAddress.port()))) {
                         continue;
                     }

                     if (!knownFailedBrokers.contains(id) ) {
                         try {
                             System.out.println("Leader " + brokerId + ": Sending heartbeat to broker " + id);
                             Socket socket = new Socket(followerAddr.ip(), followerAddr.port());
                             socket.setSoTimeout(1000);
                             ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                             out.writeObject(new Heartbeat());
                             out.flush();
                             socket.close();
                         } catch (Exception e) {
                             System.out.println("[WARNING]: Failed to send heartbeat to broker " + id + ": " + e.getMessage());
                         }
                     }
                 }

                 // Check for follower timeouts
                 List<Integer> removedBrokers = sharedState.checkFollowerTimeouts(HEARTBEAT_TIMEOUT_MS);

                 // Process broker removals
                 if (!removedBrokers.isEmpty()) {
                     System.out.println("Leader " + brokerId + ": Removing brokers: " + removedBrokers);
                     knownFailedBrokers.addAll(removedBrokers);
                     // Broadcast removals to other brokers...
                 }

                 Thread.sleep(HEARTBEAT_INTERVAL_MS);
             }
         } catch (InterruptedException e) {
             System.out.println("Leader " + brokerId + ": Heartbeat sender thread interrupted");
         } catch (Exception e) {
             System.out.println("Leader " + brokerId + ": Error in heartbeat sender: " + e.getMessage());
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
                    boolean leaderFailed = !electionInProgress && sharedState.checkLeaderTimeout(brokerId, HEARTBEAT_TIMEOUT_MS);
                    if (leaderFailed) {
                        synchronized (electionLock) {
                            if (!electionInProgress) {
                                electionInProgress = true;
                                // Add random delay to prevent all followers starting election at once
                                Thread.sleep(random.nextInt(1000));
                                startLeaderElection();
                            }
                        }
                    }

                    Thread.sleep(HEARTBEAT_TIMEOUT_MS / 3);
                }
            } catch (InterruptedException e) {
                // Exit gracefully
            }
        });
        heartbeatMonitorThread.setDaemon(true);
        heartbeatMonitorThread.start();
    }

    private static void startLeaderElection() {
        System.out.println("Broker " + brokerId + ": Starting leader election");

        int failedLeaderId = sharedState.getLeaderId();
        sharedState.removeBrokerAddress(failedLeaderId);
        System.out.println("Broker " + brokerId + ": Removed failed leader " + failedLeaderId + " from active brokers");

        // Reset election state
        currentBestCandidate = brokerId;
        currentBestLogLength = sharedState.getLogLength();
        receivedAcks.clear();
        receivedAcks.add(brokerId); // Count ourselves

        // Get only currently active brokers
        Map<Integer, Address> brokerAddresses = sharedState.getBrokerAddresses();
        System.out.println("startleaderElection: Broker " + brokerId + ": Current brokers: " + sharedState.getBrokerAddresses());

        // Remove the failed leader from consideration
        brokerAddresses.remove(sharedState.getLeaderId());

        int activeBrokerCount = brokerAddresses.size();
        int requiredAcks = activeBrokerCount / 2;  // We already counted ourselves
        System.out.println("Broker " + brokerId + ": Election among " + activeBrokerCount +
                " active brokers, expecting " + requiredAcks + " acks");

        // Send nomination to all other brokers
        NewLeaderNomination nomination = new NewLeaderNomination(brokerId, currentBestLogLength);

        for (Map.Entry<Integer, Address> entry : brokerAddresses.entrySet()) {
            System.out.println("Brokerrr " + brokerId + ": with address" + brokerAddresses.get(entry.getKey()));
            Integer targetId = entry.getKey();

            //TODO: check if the targetId is the same as the brokerId and also that the targetId has a different port from brokerId
            if (!targetId.equals(brokerId) && !Objects.equals(brokerAddresses.get(targetId).port(), brokerAddresses.get(brokerId).port())) {


                try {
                    sendMessage(entry.getValue(), nomination);
                    System.out.println("Broker " + brokerId + ": Sent nomination to broker " + targetId + "with address " + entry.getValue());
                } catch (Exception e) {
                    System.out.println("[WARNING]: Failed to send nomination to broker " + targetId + ": " + e.getMessage());
                    sharedState.removeBrokerAddress(targetId);
                }
            }
        }

        // Start a timeout for the election process
        new Thread(() -> {
            try {
                Thread.sleep(5000); // 5 second timeout
                synchronized (electionLock) {
                    if (electionInProgress && currentBestCandidate == brokerId) {
                        System.out.println("Broker " + brokerId + ": Election timed out, declaring self as leader");

                        // Update shared state
                        sharedState.setNewLeaderId(brokerId);

                        // Announce new leadership
                        NewLeaderAnnouncement announcement = new NewLeaderAnnouncement(brokerId);

                        for (Map.Entry<Integer, Address> entry : brokerAddresses.entrySet()) {
                            Integer targetId = entry.getKey();
                            if (!targetId.equals(brokerId)) {
                                try {
                                    sendMessage(entry.getValue(), announcement);
                                    System.out.println("Broker " + brokerId + ": Sent leader announcement to broker " + targetId);
                                } catch (Exception e) {
                                    System.out.println("[WARNING]: Failed to send leader announcement to broker " + targetId);
                                }
                            }
                        }

                        // Reset election state and switch to leader mode
                        electionInProgress = false;
                        stopHeartbeatThreads();
                        startHeartbeatSender();
                    }
                }
            } catch (InterruptedException e) {
                // Ignore
            }
        }).start();
    }

    private static void handleNewLeaderNomination(NewLeaderNomination msg) {
        System.out.println("Broker " + brokerId + ": Received leader nomination from broker " +
                msg.brokerId() + " with log length " + msg.logLength());

        synchronized (electionLock) {
            if (!electionInProgress) {
                electionInProgress = true;
                currentBestCandidate = brokerId;
                currentBestLogLength = sharedState.getLogLength();
                System.out.println("Broker " + brokerId + ": Starting election process, my log length: " + currentBestLogLength);
                //startLeaderElection();
            }

            // Compare log lengths
            int myLogLength = sharedState.getLogLength();
            boolean sendAck = false;

            System.out.println("Broker " + brokerId + ": Comparing log lengths - mine: " + myLogLength +
                    ", candidate " + msg.brokerId() + ": " + msg.logLength());

            if (msg.logLength() > myLogLength) {
                // The other broker has a longer log, they should be the leader
                currentBestCandidate = msg.brokerId();
                currentBestLogLength = msg.logLength();
                sendAck = true;
                System.out.println("Broker " + brokerId + ": Candidate has longer log, will send ACK");
                //sendNominationAck(msg.brokerId());
            } else if (myLogLength > msg.logLength()) {
                // My log is longer, I should be the leader - send my nomination
                System.out.println("Broker " + brokerId + ": My log is longer, sending my nomination");
                Address candidateAddr = sharedState.getBrokerAddress(msg.brokerId());

                if (candidateAddr != null) {
                    try {
                        NewLeaderNomination myNomination = new NewLeaderNomination(brokerId, myLogLength);
                        sendMessage(candidateAddr, myNomination);
                        System.out.println("Broker " + brokerId + ": Sent my nomination to broker " + msg.brokerId());
                    } catch (Exception e) {
                        System.out.println("[WARNING]: Failed to send nomination to broker " + msg.brokerId() + ": " + e.getMessage());
                    }
                } else {
                    System.out.println("[WARNING]: Could not find address for broker " + msg.brokerId());
                }
            } else {
                // Equal log lengths, use broker ID as tiebreaker
                System.out.println("Broker " + brokerId + ": Equal log lengths, using broker ID as tiebreaker");
                if (msg.brokerId() > brokerId) {
                    currentBestCandidate = msg.brokerId();
                    sendAck = true;
                    System.out.println("Broker " + brokerId + ": Candidate has higher ID, will send ACK");
                    //sendNominationAck(msg.brokerId());
                } else {
                    System.out.println("Broker " + brokerId + ": I have higher ID, no ACK needed");
                }
            }

            // Send ACK if the other broker has a better candidacy
            if (sendAck) {
                Address candidateAddr = sharedState.getBrokerAddress(msg.brokerId());
                if (candidateAddr != null) {
                    try {
                        sendMessage(candidateAddr, new NewLeaderNominationAck(brokerId));
                        System.out.println("Broker " + brokerId + ": Sent nomination ACK to broker " + msg.brokerId());
                    } catch (Exception e) {
                        System.out.println("[WARNING]: Failed to send nomination ACK to broker " + msg.brokerId() + ": " + e.getMessage());
                    }
                } else {
                    System.out.println("[WARNING]: Could not find address for broker " + msg.brokerId());
                }
            }
        }
    }
    private static void sendNominationAck(int candidateId) {
        Address candidateAddr = sharedState.getBrokerAddress(candidateId);
        if (candidateAddr != null) {
            try {
                sendMessage(candidateAddr, new NewLeaderNominationAck(brokerId));
                System.out.println("Broker " + brokerId + ": Sent nomination ACK to broker " + candidateId);
            } catch (Exception e) {
                System.out.println("[WARNING]: Failed to send nomination ACK: " + e.getMessage());
            }
        } else {
            System.out.println("[WARNING]: Could not find address for broker " + candidateId);
        }
    }

    private static void handleNewLeaderNominationAck(NewLeaderNominationAck msg) {
        synchronized (electionLock) {
            // Only process ACKs if we're a candidate
            if (currentBestCandidate == brokerId) {
                System.out.println("Broker " + brokerId + ": Received nomination ACK from broker " + msg.senderId());
                receivedAcks.add(msg.senderId());

                // Count the number of active brokers (excluding the crashed leader)
                Map<Integer, Address> brokerAddresses = sharedState.getBrokerAddresses();
                System.out.println("handlenewleadernomack: Broker " + brokerId + ": Current brokers: " + sharedState.getBrokerAddresses());
                brokerAddresses.remove(sharedState.getLeaderId()); // Remove failed leader
                int activeCount = brokerAddresses.size();

                System.out.println("Broker " + brokerId + ": Current ACK count: " +
                        receivedAcks.size() + "/" + activeCount + " (including self)");

                // Check if we have received acks from majority of active brokers
                if (receivedAcks.size() >= (activeCount / 2 + 1)) {
                    // We're the new leader!
                    System.out.println("Broker " + brokerId + ": Received majority of ACKs, becoming new leader");

                    // Update shared state
                    sharedState.setNewLeaderId(brokerId);

                    // Announce new leadership
                    NewLeaderAnnouncement announcement = new NewLeaderAnnouncement(brokerId);

                    for (Map.Entry<Integer, Address> entry : brokerAddresses.entrySet()) {
                        Integer targetId = entry.getKey();
                        if (!targetId.equals(brokerId)) {
                            try {
                                sendMessage(entry.getValue(), announcement);
                                System.out.println("Broker " + brokerId + ": Sent leader announcement to broker " + targetId);
                            } catch (Exception e) {
                                System.out.println("[WARNING]: Failed to send leader announcement to broker " + targetId + ": " + e.getMessage());
                            }
                        }
                    }

                    // Reset election state and switch to leader mode
                    electionInProgress = false;
                    stopHeartbeatThreads();
                    startHeartbeatSender();
                }
            } else {
                System.out.println("Broker " + brokerId + ": Received ACK from " + msg.senderId() +
                        " but I'm not the best candidate (current best: " + currentBestCandidate + ")");
            }
        }
    }

  /*  private static void handleNewLeaderAnnouncement(NewLeaderAnnouncement msg) {
        System.out.println("Broker " + brokerId + ": Received leader announcement from broker " + msg.brokerId());

        // Update leader info
        sharedState.setNewLeaderId(msg.brokerId());

        // Reset election state
        synchronized (electionLock) {
            electionInProgress = false;
            currentBestCandidate = -1;
            currentBestLogLength = -1;
            receivedAcks.clear();
        }

        // Restart heartbeat mechanism in follower mode
        stopHeartbeatThreads();
        startHeartbeatMonitor();
    }  */
  private static void handleNewLeaderAnnouncement(NewLeaderAnnouncement msg) {
      System.out.println("Broker " + brokerId + ": Received leader announcement from broker " + msg.brokerId());

      // First stop all heartbeat threads to avoid conflicts
      stopHeartbeatThreads();

      // Update leader info
      sharedState.setNewLeaderId(msg.brokerId());

      // Reset election state
      synchronized (electionLock) {
          electionInProgress = false;
          currentBestCandidate = -1;
          currentBestLogLength = -1;
          receivedAcks.clear();
      }

      if (msg.brokerId() != brokerId) {
          try {
              Address leaderAddr = sharedState.getBrokerAddress(msg.brokerId());
              if (leaderAddr != null) {
                  sendMessage(leaderAddr, new HeartbeatAck(brokerId));
                  System.out.println("Broker " + brokerId + ": Sending initial heartbeat ack to new leader " + msg.brokerId());
              }
          } catch (Exception e) {
              System.out.println("[WARNING]: Failed to send initial heartbeat ack: " + e.getMessage());
          }

          // Start heartbeat monitor only if I'm not the leader
          startHeartbeatMonitor();
          System.out.println("Broker " + brokerId + ": Switched to follower mode, monitoring leader " + msg.brokerId());
      } else {
          // I am the leader, start heartbeat sender
          startHeartbeatSender();
          System.out.println("Broker " + brokerId + ": I am the new leader, starting heartbeat sender");
          //TODO: here print the list of brokers and their addresses
            System.out.println("Broker " + brokerId + ": Current brokers: " + sharedState.getBrokerAddresses());
      }
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

   /* private static void addBroker(BrokerJoinRequest req) {
        // get a new broker id from the shared state
        int newBrokerId = sharedState.getNewBrokerId();

        // add the broker to the list in the shared state
        sharedState.addBrokerAddress(newBrokerId, req.brokerAddress());

        // if leader, return a BrokerJoinResponse to the requesting broker
        if (isLeader()) {
            sendMessage(req.brokerAddress(), new BrokerJoinResponse(newBrokerId, sharedState));
        }
    } */
   private static void addBroker(BrokerJoinRequest req) {
       // First check if this broker's address already exists
       int newBrokerId = -1;
       Map<Integer, Address> existingBrokers = sharedState.getBrokerAddresses();

       // Check if the address already exists in the system
       for (Map.Entry<Integer, Address> entry : existingBrokers.entrySet()) {
           Address existingAddr = entry.getValue();
           if (existingAddr.ip().equals(req.brokerAddress().ip()) &&
                   Objects.equals(existingAddr.port(), req.brokerAddress().port())) {
               // Address already exists, use the existing broker ID
               newBrokerId = entry.getKey();
               System.out.println("[INFOo]: Broker with address " + req.brokerAddress() +
                       " already exists with ID " + newBrokerId);
               break;
           }
       }

       // If no existing broker with this address, get a new broker ID
       if (newBrokerId == -1) {
           newBrokerId = sharedState.getNewBrokerId();
           // Add the broker to the list in the shared state
           sharedState.addBrokerAddress(newBrokerId, req.brokerAddress());
       }

       // If leader, return a BrokerJoinResponse to the requesting broker
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