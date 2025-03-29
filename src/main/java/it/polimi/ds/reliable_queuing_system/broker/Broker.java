package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.*;
import it.polimi.ds.reliable_queuing_system.utils.Address;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private static AtomicBoolean electionInProgress = new AtomicBoolean(false);

//    private static volatile boolean electionInProgress = false;
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
            System.out.println("[INFO]: First, Broker " + brokerId + " started with address: " + brokerAddress);
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
            System.out.println("[INFO]: Broker ready to receive messages at port: " + brokerAddress.port());

            HeartbeatManager heartbeatManager = new HeartbeatManager(brokerId, sharedState, electionInProgress);
            LogManager logManager = new LogManager(brokerId, sharedState);
            MessageDispatcher messageDispatcher = new MessageDispatcher(brokerId, sharedState, heartbeatManager, logManager);

            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                ObjectInputStream in  = new ObjectInputStream(socket.getInputStream());

                try {
                    Message message = (Message) in.readObject();

                    if (brokerState == BrokerState.WAITING_JOIN) {
                        if (message instanceof BrokerJoinResponse brokerJoinResponse) {
                            brokerId = brokerJoinResponse.newBrokerId();
                            System.out.println("[INFO]: Not first, Broker " + brokerId + " started with address: " + brokerAddress);
                            sharedState = brokerJoinResponse.sharedState();
                            brokerState = BrokerState.READY;
                        }
                        else {
                            System.out.println("[INFO]: Received message before joining the cluster. It will be ignored.");
                        }
                    }
                    else {
                        messageDispatcher.dispatch(message);
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

    //endregion

    //TODO: replace the (now broken) functions below with their proper implementation after refactoring

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


        // Remove the failed leader from consideration
        brokerAddresses.remove(sharedState.getLeaderId());

        // Send nomination to all other brokers
        NewLeaderNomination nomination = new NewLeaderNomination(brokerId, currentBestLogLength);

        for (Map.Entry<Integer, Address> entry : brokerAddresses.entrySet()) {

            Integer targetId = entry.getKey();

            try {
                sendMessage(entry.getValue(), nomination);
                System.out.println("Broker " + brokerId + ": Sent nomination to broker " + targetId + "with address " + entry.getValue());
            } catch (Exception e) {
                System.out.println("[WARNING]: Failed to send nomination to broker " + targetId + ": " + e.getMessage());
                //sharedState.removeBrokerAddress(targetId);
            }

        }

        // Start a timeout for the election process
        new Thread(() -> {
            try {
                Thread.sleep(5000);
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
        System.out.println("Broker " + brokerId + ": Received leader nomination from broker " + msg.brokerId() + " with log length " + msg.logLength());

        synchronized (electionLock) {
            if (!electionInProgress) {
                electionInProgress = true;
                currentBestCandidate = brokerId;
                currentBestLogLength = sharedState.getLogLength();
            }

            // Compare log lengths
            int myLogLength = sharedState.getLogLength();
            boolean sendAck = false;

            System.out.println("Broker " + brokerId + ": Comparing log lengths - mine: " + myLogLength + ", candidate " + msg.brokerId() + ": " + msg.logLength());

            if (msg.logLength() > myLogLength) {
                // The other broker has a longer log, they should be the leader
                currentBestCandidate = msg.brokerId();
                currentBestLogLength = msg.logLength();
                sendAck = true;
                System.out.println("Broker " + brokerId + ": Candidate has longer log, will send ACK");

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

    private static void handleNewLeaderNominationAck(NewLeaderNominationAck msg) {
        synchronized (electionLock) {
            // Only process ACKs if we're a candidate
            if (currentBestCandidate == brokerId) {
                System.out.println("Broker " + brokerId + ": Received nomination ACK from broker " + msg.senderId());
                receivedAcks.add(msg.senderId());

                // Count the number of active brokers (excluding the crashed leader)
                Map<Integer, Address> brokerAddresses = sharedState.getBrokerAddresses();
                brokerAddresses.remove(sharedState.getLeaderId()); // Remove failed leader
                int activeCount = brokerAddresses.size();

                System.out.println("Broker " + brokerId + ": Current ACK count: " + receivedAcks.size() + "/" + activeCount + " (including self)");

                // Check if we have received acks from the majority of active brokers
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
                System.out.println("Broker " + brokerId + ": Received ACK from " + msg.senderId() + " but I'm not the best candidate (current best: " + currentBestCandidate + ")");
            }
        }
    }

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

      }
  }
}