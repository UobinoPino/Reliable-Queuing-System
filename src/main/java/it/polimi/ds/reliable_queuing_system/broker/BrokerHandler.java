//package it.polimi.ds.reliable_queuing_system.broker;
//
//import it.polimi.ds.reliable_queuing_system.messages.*;
//
//import java.io.EOFException;
//import java.io.IOException;
//import java.io.ObjectInputStream;
//import java.io.ObjectOutputStream;
//import java.net.Socket;
//
//public class BrokerHandler implements Runnable{
//
//    public BrokerHandler(Socket socket, int myId, SharedState sharedState) throws IOException {
//        this.myId = myId;
//        this.sharedState = sharedState;
//        this.toBroker = new ObjectOutputStream(socket.getOutputStream());
//        toBroker.flush();
//        this.fromBroker = new ObjectInputStream(socket.getInputStream());
//    }
//    private final int myId;
//    private final SharedState sharedState;
//    private final ObjectInputStream fromBroker;
//    private final ObjectOutputStream toBroker;
//    public void run() {
//        while (true) {
//            try {
//                Message msg = (Message) fromBroker.readObject();
//
//                if (msg == null) {
//                    break;
//                }
//                //mega switch
//                switch (msg) {
//
//                    case BrokerAddition brokerAddition -> handleBrokerAddition(brokerAddition);
//                    case BrokerAdditionAck brokerAdditionAck -> handleBrokerAdditionAck(brokerAdditionAck);
//                    case BrokerJoinRequest joinRequest -> handleBrokerJoinRequest(joinRequest);
//                    case BrokerJoinResponse joinResponse -> handleBrokerJoinResponse(joinResponse);
//                    case BrokerRemoval brokerRemoval -> handleBrokerRemoval(brokerRemoval);
//                    case ClientIdAssignment assignment -> handleClientIdAssignment(assignment);
//                    case ClientIdRequest clientIdRequest -> forwardToLeader(clientIdRequest);
//
//                    case Heartbeat heartbeat -> handleHeartbeat(heartbeat);
//                    case HeartbeatAck heartbeatAck -> handleHeartbeatAck(heartbeatAck);
//
//                    case EntryPropagation entryPropagation -> handleEntryPropagation(entryPropagation);
//                    case EntryCommit entryCommit -> handleEntryCommit(entryCommit);
//
//                    /*
//
//                    case ReadRequest readRequest -> forwardToLeader(readRequest);
//                    case ReadResponse readResponse -> handleReadResponse(readResponse);
//                    case ReadConfirmation readConfirmation -> handleReadConfirmation(readConfirmation);
//                    case WriteRequest writeRequest -> forwardToLeader(writeRequest);
//                    case WriteResponse writeResponse -> handleWriteResponse(writeResponse);
//
//                    case ClientOffsetsUpdate offsetsUpdate -> handleClientOffsetsUpdate(offsetsUpdate);
//
//                    case EntryPropagationAck ack -> handleEntryPropagationAck(ack);
//
//                    case NewLeaderAnnouncement announcement -> handleNewLeaderAnnouncement(announcement);
//                    case NewLeaderNomination nomination -> handleNewLeaderNomination(nomination);
//                    case NewLeaderNominationAck nominationAck -> handleNewLeaderNominationAck(nominationAck);
//
//
//                    */
//                    default -> System.out.println("Unknown message type: Leonardo abbiamo aggiunto altri messaggi?" + msg.getClass().getSimpleName());
//
//                }
//            }
//            catch (ClassNotFoundException | ClassCastException ignored) {
//                // Ignore unknown message types
//            }
//            catch (EOFException e) {
//                // Connection closed
//                break;
//            }
//            catch (IOException e) {
//                e.printStackTrace();
//                break;
//            }
//        }
//    }
//    private void handleBrokerAddition(BrokerAddition brokerAddition) {
//        // Update the shared state to include the new broker's information
//        sharedState.addBrokerAddress(brokerAddition.brokerId(),
//                brokerAddition.brokerIp() + ":" + brokerAddition.brokerPort());
//
//        try {
//            // Send acknowledgment back
//            toBroker.writeObject(new BrokerAdditionAck(brokerAddition.brokerId()));
//            toBroker.flush();
//        } catch (IOException e) {
//            System.out.println("Failed to send BrokerAdditionAck: " + e.getMessage());
//        }
//    }
//
//    private void handleBrokerAdditionAck(BrokerAdditionAck brokerAdditionAck) {
//        // Record that a broker has acknowledged the addition
//        System.out.println("Received acknowledgment for broker addition: " + brokerAdditionAck.brokerId());
//
//    }
//
//    private void handleBrokerJoinRequest(BrokerJoinRequest joinRequest) {
//        if (sharedState.isLeader(myId)) {
//            // This broker is the leader, handle the join request directly
//            try {
//                // Assign a new broker ID
//                int newBrokerId = sharedState.getNewBrokerId();
//
//                // Add broker to shared state
//                sharedState.addBrokerAddress(newBrokerId,
//                        joinRequest.brokerIp() + ":" + joinRequest.brokerPort());
//
//                // Send shared state back to the joining broker
//                // TODO: maybe better to wait for at least one BrokerAdditionAck before sending the JoinResponse?
//                //  otherwise those acks are useless (?) Idk
//                toBroker.writeObject(new BrokerJoinResponse(newBrokerId, sharedState));
//                toBroker.flush();
//
//                // Notify other followers about the new broker
//                notifyFollowersAboutNewBroker(joinRequest.brokerIp(), joinRequest.brokerPort(), newBrokerId);
//            } catch (IOException e) {
//                System.out.println("Failed to handle broker join request: " + e.getMessage());
//            }
//        } else {
//            // Not the leader, forward to the leader
//            forwardToLeader(joinRequest);
//        }
//    }
//
//    private void handleBrokerJoinResponse(BrokerJoinResponse joinResponse) {
//        // Update local shared state with the received state
//        sharedState.updateFrom(joinResponse.sharedState());
//        System.out.println("Updated shared state from leader's response");
//    }
//
//    private void handleBrokerRemoval(BrokerRemoval brokerRemoval) {
//        // Remove the broker from shared state
//        sharedState.removeBrokerAddress(brokerRemoval.brokerId());
//        System.out.println("Broker " + brokerRemoval.brokerId() + " has been removed from the system");
//    }
//
//    private void handleClientIdAssignment(ClientIdAssignment assignment) {
//        // This is  forwarded to the client
//        // In this handler, it's likely received from the leader to be forwarded to a client
//        System.out.println("Received client ID assignment: " + assignment.clientId());
//        //TODO: code the actual method
//    }
//
//    private void forwardToLeader(Message message) {
//        try {
//            // Get leader's address
//            int leaderId = sharedState.getLeaderId();
//            String leaderAddress = sharedState.getBrokerAddress(leaderId);
//
//            if (leaderAddress != null) {
//                String[] addressParts = leaderAddress.split(":");
//                String leaderIp = addressParts[0];
//                int leaderPort = Integer.parseInt(addressParts[1]);
//
//                // Connect to leader
//                try (Socket leaderSocket = new Socket(leaderIp, leaderPort)) {
//                    ObjectOutputStream toLeader = new ObjectOutputStream(leaderSocket.getOutputStream());
//                    toLeader.flush();
//                    ObjectInputStream fromLeader = new ObjectInputStream(leaderSocket.getInputStream());
//
//                    // Forward the message
//                    toLeader.writeObject(message);
//                    toLeader.flush();
//
//                    // Handle the response based on the message type
//                    if (message instanceof BrokerJoinRequest) {
//                        // For BrokerJoinRequest, we expect a BrokerJoinResponse
//                        Object response = fromLeader.readObject();
//                        if (response instanceof BrokerJoinResponse joinResponse) {
//                            // Update our shared state with the leader's response
//                            // TODO: maybe not needed? since all followers already updated their state after receiving BrokerAddition message? But I'm not sure
//                            sharedState.updateFrom(joinResponse.sharedState());
//
//                            // Forward the response back to the original broker
//                            toBroker.writeObject(joinResponse);
//                            toBroker.flush();
//                        } else {
//                            System.out.println("Unexpected response from leader for BrokerJoinRequest");
//                        }
//                    } else if (message instanceof ClientIdRequest) {
//                        // For ClientIdRequest, we expect a ClientIdAssignment
//                        Object response = fromLeader.readObject();
//                        if (response instanceof ClientIdAssignment assignment) {
//                            // Forward the client ID assignment back to the requesting client
//                            toBroker.writeObject(assignment);
//                            toBroker.flush();
//                        } else {
//                            System.out.println("Unexpected response from leader for ClientIdRequest");
//                        }
//                    }
//                    // Additional message types can be handled here as needed
//                } catch (ClassNotFoundException e) {
//                    System.out.println("Error reading response from leader: " + e.getMessage());
//                }
//            } else {
//                System.out.println("Leader address not found in shared state");
//            }
//        } catch (IOException e) {
//            System.out.println("Failed to forward message to leader: " + e.getMessage());
//        }
//    }
//
//    // Helper method to notify all followers about a new broker
//    private void notifyFollowersAboutNewBroker(String brokerIp, int brokerPort, int brokerId) {
//        // Iterate through all broker IDs first to get all known brokers
//        for (int i = 0; i < sharedState.getNewBrokerId(); i++) {
//            // Skip self and the new broker
//            if (i == myId || i == brokerId) {
//                continue;
//            }
//
//            // Get the address for this broker ID
//            String address = sharedState.getBrokerAddress(i);
//            if (address == null) {
//                continue; // This broker ID might no longer exist
//            }
//
//            try {
//                String[] addressParts = address.split(":");
//                String followerIp = addressParts[0];
//                int followerPort = Integer.parseInt(addressParts[1]);
//
//                try (Socket followerSocket = new Socket(followerIp, followerPort)) {
//                    ObjectOutputStream toFollower = new ObjectOutputStream(followerSocket.getOutputStream());
//                    toFollower.flush();
//
//                    // Send notification about new broker
//                    toFollower.writeObject(new BrokerAddition(brokerIp, brokerPort, brokerId));
//                    toFollower.flush();
//
//                }
//            } catch (IOException e) {
//                System.out.println("Failed to notify follower " + i + ": " + e.getMessage());
//            }
//        }
//    }
//
//    private void handleHeartbeat(Heartbeat heartbeat) {
//        // We received a heartbeat. Typically, it's from the leader to a follower.
//        // Update last time the follower saw a heartbeat from the leader.
//        // Then respond with a HeartbeatAck so the leader knows we are alive.
//        sharedState.updateLastHeartbeatReceived(myId);  // track that *this* broker got a heartbeat
//        try {
//            toBroker.writeObject(new HeartbeatAck());
//            toBroker.flush();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    private void handleHeartbeatAck(HeartbeatAck heartbeatAck) {
//        // This is typically a leader receiving ack from a follower.
//        sharedState.updateLastHeartbeatAckReceived(myId); // leader sees ack from this connection
//    }
//
//    private void handleEntryPropagation(EntryPropagation entryPropagation) throws IOException {
//        sharedState.addEntryWaitingCommit(entryPropagation.logIndex(), entryPropagation.logEntry());
//
//        toBroker.writeObject(new EntryPropagationAck(entryPropagation.logIndex(), entryPropagation.logEntry()));
//        toBroker.flush();
//    }
//
//    private void handleEntryPropagationAck(EntryPropagationAck entryPropagationAck) {
//        // if the log entry related to the received ack was still waiting for the first ack to arrive,
//        // remove it from the queue and send its EntryCommit message
//        if (sharedState.isEntryWaitingAck(entryPropagationAck.logIndex())) {
//            sharedState.removeEntryWaitingAck(entryPropagationAck.logIndex());
//
//        }
//
//        // update value, offsets and log locally
//
//        // send EntryCommit to all other brokers
//    }
//
//    private void handleEntryCommit(EntryCommit entryCommit) {
//        sharedState.commitLogEntry(entryCommit.logIndex());
//    }
//}
//
