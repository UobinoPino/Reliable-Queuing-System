package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.*;
import it.polimi.ds.reliable_queuing_system.utils.Address;
import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;

/// A class that will take care of handling messages received by the broker.
public class MessageDispatcher {
    public MessageDispatcher(int brokerId, SharedState sharedState, HeartbeatManager heartbeatManager, LogManager logManager, ElectionInfo electionInfo) {
        this.brokerId = brokerId;
        this.sharedState = sharedState;
        this.heartbeatManager = heartbeatManager;
        this.logManager = logManager;
        this.electionInfo = electionInfo;
        this.delayedMessages = new ConcurrentLinkedQueue<>();
    }

    private final int brokerId;
    private final SharedState sharedState;
    private final HeartbeatManager heartbeatManager;
    private final LogManager logManager;

    private final ElectionInfo electionInfo;
    private final Queue<Message> delayedMessages;

    /// Dispatch the given [Message] to its dedicated handler method.
    public void dispatch(Message message) throws ClassNotFoundException {
        // if there is an election ongoing, ignore messages unrelated to the election,
        // and store them so that they will be handled after the election
        if (electionInfo.isElectionInProgress() && !isElectionRelated(message)) {
            delayedMessages.add(message);
            return;
        }

        // else, handle the message accordingly
        switch (message) {
            case EntryPropagation msg -> handleEntryPropagation(msg);
            case EntryPropagationAck msg -> handleEntryPropagationAck(msg);
            case EntryCommit msg -> handleEntryCommit(msg);
            case BrokerJoinRequest msg -> handleGenericRequest(msg);
            case ClientIdRequest msg -> handleGenericRequest(msg);
            case ReadRequest msg -> handleReadRequest(msg);
            case ClientOffsetsUpdateRequest msg -> handleGenericRequest(msg);
            case WriteRequest msg -> handleGenericRequest(msg);
            case BrokerRemoval msg -> handleBrokerRemoval(msg);
            case Heartbeat msg -> handleHeartbeat(msg);
            case HeartbeatAck msg -> handleHeartbeatAck(msg);
            case NewLeaderNomination msg -> handleNewLeaderNomination(msg);
            case NewLeaderNominationAck msg -> handleNewLeaderNominationAck(msg);
            case NewLeaderAnnouncement msg -> handleNewLeaderAnnouncement(msg);
            default -> throw new ClassNotFoundException();
        }
    }

    /// Returns whether the broker is the current leader of the system or not.
    private boolean isLeader() {
        return sharedState.getLeaderId() == brokerId;
    }

    /// Returns whether the given message is related to the election of a leader or not.
    private boolean isElectionRelated(Message msg) {
        return msg instanceof NewLeaderNomination || msg instanceof NewLeaderNominationAck || msg instanceof NewLeaderAnnouncement;
    }

    /// Returns true if the second candidate provided is better than the first one, false otherwise.
    private boolean compareCandidates(int id1, int logLength1, int id2, int logLength2) {
        if (logLength1 > logLength2) {
            return false;
        } else if (logLength1 < logLength2) {
            return true;
        } else {
            return id2 > id1;
        }
    }

    /// Handler method for received [EntryPropagation] messages.
    private void handleEntryPropagation(EntryPropagation msg) {
        if (!isLeader()) {
            // store the entry as waiting for commit
            sharedState.addWaitingCommitEntry(msg.logEntry());

            // send an ACK to the leader
            Address leaderAddr = sharedState.getBrokerAddress(sharedState.getLeaderId());
            NetworkManager.sendMessage(new EntryPropagationAck(msg.logEntry()), leaderAddr);
        }
    }

    /// Handler method for received [EntryPropagationAck] messages.
    private void handleEntryPropagationAck(EntryPropagationAck msg) {
        if (isLeader()) {
            if(sharedState.isEntryWaitingAck(msg.logEntry())) {
                // commit the entry locally
                logManager.commitEntry(msg.logEntry());

                // broadcast the EntryCommit message
                //FIXME: this is probably what causes the new followers to adding themselves twice
                NetworkManager.broadcastMessage(new EntryCommit(msg.logEntry()), brokerId, sharedState);
            }
        }
    }

    /// Handler method for received [EntryCommit] messages.
    private void handleEntryCommit(EntryCommit msg) {
        if (!isLeader()) {
            // commit the entry locally
            logManager.commitEntry(msg.logEntry());
        }
    }

    /// Handler method for received messages that doesn't need custom handler.
    /// (They will be propagated and wait for an ack from another node).
    private void handleGenericRequest(Message msg) {
        if (isLeader()) {
            // create a new log entry for given request
            LogEntry newEntry = new LogEntry(sharedState.getLogLength(), msg);

            // if multi-broker system, store the entry as waiting for ACK and propagate it
            if (sharedState.getBrokersCount() > 1) {
                sharedState.addWaitingAckEntry(newEntry);
                NetworkManager.broadcastMessage(new EntryPropagation(newEntry), brokerId, sharedState);
            }
            // else (single-broker system) commit the entry locally
            else {
                logManager.commitEntry(newEntry);
            }
        }
        else {
            NetworkManager.forwardMessageToLeader(msg, brokerId, sharedState);
        }
    }

    /// Handler for received [ReadRequest] messages.
    private void handleReadRequest(ReadRequest msg) {
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
        NetworkManager.sendMessage(new ReadResponse(msg.clientId(), msg.operationId(), valuesToReturn), msg.clientAddress());

        // create a new ClientOffsetUpdate message and handle it accordingly
        ClientOffsetsUpdateRequest offsetsUpdateMsg = new ClientOffsetsUpdateRequest(msg.queueName(), requestedQueue.size(), msg.clientId(), msg.operationId(), msg.clientAddress());
        handleGenericRequest(offsetsUpdateMsg);
    }

    /// Handler for received [BrokerRemoval] messages.
    private void handleBrokerRemoval(BrokerRemoval msg) {
        //TODO: maybe broker removal should be handled using log entries as well? Otherwise it wouldn't appear in the log...

        // Only process if we're not the leader (leader already removed the broker before broadcasting the BrokerRemoval message)
        if (!isLeader()) {
            sharedState.removeBrokerAddress(msg.brokerId());
            System.out.println("[INFO]: Follower " + brokerId + ": Received broker removal notification for broker " + msg.brokerId());
        }
    }

    /// Handler for received [Heartbeat] messages.
    private void handleHeartbeat(Heartbeat msg) {
        // First check if an election is in progress
        if (electionInfo.isElectionInProgress()) {
            System.out.println("[INFO]: Ignoring heartbeat since an election is in progress");
            return;
        }

        if (!isLeader()) {
            System.out.println("[INFO]: Received heartbeat from leader");

            // Update timestamp in shared state
            heartbeatManager.updateLastHeartbeatReceived();

            // Send HeartbeatAck to the leader
            try {
                NetworkManager.forwardMessageToLeader(new HeartbeatAck(brokerId), brokerId, sharedState);
            } catch (RuntimeException e) {
                System.out.println("[WARN]: Failed to send heartbeat acknowledgment. Leader may have crashed.");
                // If we failed to connect to the leader, consider starting an election
                if (!electionInfo.isElectionInProgress()) {
                    // Initialize active brokers from sharedState for the election
                    //electionInfo.updateActiveBrokers(sharedState.getBrokerAddresses().keySet());
                    System.out.println("[INFO]: Initiating election due to connection failure to leader");
                    // Similar logic to heartbeatMonitorTask - start election process
                    sharedState.removeBrokerAddress(sharedState.getLeaderId());

                    if (electionInfo.wasElectionInProgress()) {
                        int myLogLength = sharedState.getLogLength();
                        electionInfo.updateBestCandidate(brokerId, myLogLength);
                        NetworkManager.broadcastMessage(new NewLeaderNomination(brokerId, myLogLength), brokerId, sharedState);

                        Address myAddress = sharedState.getBrokerAddress(brokerId);
                        NetworkManager.sendMessage(new NewLeaderNominationAck(brokerId), myAddress);
                    }
                }
            }
        }
    }

    /// Handler for received [HeartbeatAck] messages.
    private void handleHeartbeatAck(HeartbeatAck msg) {
        if (isLeader()) {
            Integer followerId = msg.brokerId();
            if (followerId != null) {
                heartbeatManager.updateLastHeartbeatAckReceived(followerId);
                System.out.println("[INFO]: Received heartbeat acknowledgment from broker " + followerId);
            } else {
                //TODO: is this necessary? Like, can the followerId inside a HeartbeatAck be null?
                // (if so maybe we should check validity of parameters of all other messages)
                System.out.println("[ERROR]: Received heartbeat acknowledgment without broker ID");
            }
        }
    }

    /// Handler for received [NewLeaderNomination] messages.
    private void handleNewLeaderNomination(NewLeaderNomination msg) {
        System.out.println("[INFO]: Received leader nomination from broker " + msg.brokerId() + " with log length " + msg.logLength());

        // If this is the first nomination received...
        if (electionInfo.wasElectionInProgress()) {
            // remove the leader from the list of known brokers
            sharedState.removeBrokerAddress(sharedState.getLeaderId());


            // compare your log with the received one
            int myLogLength = sharedState.getLogLength();

            System.out.println("[INFO]: Comparing log lengths - mine: " + myLogLength + ", candidate (" + msg.brokerId() + "): " + msg.logLength());

            boolean candidateIsBetter = compareCandidates(brokerId, myLogLength, msg.brokerId(), msg.logLength());

            // if received is better...
            if (candidateIsBetter) {
                System.out.println("[INFO]: Candidate is better, will send ACK");

                // update best-candidate accordingly
                electionInfo.updateBestCandidate(msg.brokerId(), msg.logLength());

                // send ACK for the received nomination
                Address senderAddr = sharedState.getBrokerAddress(msg.brokerId());
                NetworkManager.sendMessage(new NewLeaderNominationAck(brokerId), senderAddr);
            }
            // else...
            else {
                System.out.println("[INFO]: I'm better, will broadcast nomination");

                // set self as best candidate and broadcast nomination
                electionInfo.updateBestCandidate(brokerId, myLogLength);
                NetworkManager.broadcastMessage(new NewLeaderNomination(brokerId, myLogLength), brokerId, sharedState);

                // send to self an ACK for own nomination
                Address myAddress = sharedState.getBrokerAddress(brokerId);
                NetworkManager.sendMessage(new NewLeaderNominationAck(brokerId), myAddress);
            }
        }
        // else (an election was already in progress)...
        else {
            System.out.println("[INFO]: Comparing log lengths - stored best ("+ electionInfo.getBestCandidate() +"): " + electionInfo.getBestCandidateLogLength() + ", candidate (" + msg.brokerId() + "): " + msg.logLength());

            // compared received log with the stored best one
            boolean candidateIsBetter = compareCandidates(electionInfo.getBestCandidate(), electionInfo.getBestCandidateLogLength(), msg.brokerId(), msg.logLength());

            // if received is better, update best accordingly and send ACK
            if (candidateIsBetter) {
                System.out.println("[INFO]: Candidate is better, will send ACK");

                electionInfo.updateBestCandidate(msg.brokerId(), msg.logLength());

                Address senderAddr = sharedState.getBrokerAddress(msg.brokerId());
                NetworkManager.sendMessage(new NewLeaderNominationAck(brokerId), senderAddr);
            }
            // else, simply ignore the nomination
            else {
                System.out.println("[INFO]: stored best is better, ignoring received nomination");
            }
        }
    }

    /// Handler for received [NewLeaderNominationAck] messages.
    private void handleNewLeaderNominationAck(NewLeaderNominationAck msg) {
        // if an election is in progress and I'm the best candidate...
        if (electionInfo.isElectionInProgress() && electionInfo.getBestCandidate() == brokerId) {
            // add the id of the sender to the set of received ACKs
            electionInfo.addReceivedAck(msg.senderId());

            System.out.println("[INFO]: Received nomination ACK from broker " + msg.senderId());

            int receivedAcksCount = electionInfo.getReceivedAcksCount();
            int activeBrokersCount = sharedState.getBrokersCount();


            System.out.println("[INFO]: Current ACK count: " + receivedAcksCount + "/" + activeBrokersCount);

            // if all ACKs have been received...
            if (receivedAcksCount >= activeBrokersCount) {  //TODO: is the total number of brokers necessary? or is the majority enough?
                System.out.println("[INFO]: Consensus achieved. Becoming new leader");
                System.out.println("[INFO]: Received ACKs from all " + activeBrokersCount +
                        " active brokers. Becoming new leader");

                // become leader
                sharedState.setNewLeaderId(brokerId);

                // broadcast new leader announcement
                List<LogEntry> completeLog = sharedState.getCompleteLog();
                NetworkManager.broadcastMessage(new NewLeaderAnnouncement(brokerId, completeLog), brokerId, sharedState);

                // terminate the election phase
                electionInfo.stopElection();

                // restart the heartbeat manager
                heartbeatManager.restart();

                // dispatch the received messages that had been delayed during the election
                for (Message delayedMessage : delayedMessages) {
                    try {
                        dispatch(delayedMessage);
                    } catch (ClassNotFoundException ignored) {
                        System.out.println("[INFO]: Unknown message was received during election, it will be ignored.");
                    }
                }
            }
        }
    }

    /// Handler for received [NewLeaderAnnouncement] messages.
    private void handleNewLeaderAnnouncement(NewLeaderAnnouncement msg) {
        if (electionInfo.isElectionInProgress()) {
            System.out.println("[INFO]: Received leader announcement from broker " + msg.brokerId());

            // update leader info in SharedState
            sharedState.setNewLeaderId(msg.brokerId());

            // Replace local log with leader's log
            System.out.println("[INFO]: Replacing log with the one received from new leader, with " + msg.newLeaderLog().size() + " entries");
            sharedState.replaceLog(msg.newLeaderLog());

            // terminate the election phase
            electionInfo.stopElection();

            // restart the heartbeat manager
            heartbeatManager.restart();  //TODO: useless? since the role of the brokers who received the announcement should be remained follower as it was before...
        }
    }
}
