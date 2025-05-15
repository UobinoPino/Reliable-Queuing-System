package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.*;
import it.polimi.ds.reliable_queuing_system.utils.Address;
import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

/// A class that will take care of handling messages received by the broker.
public class MessageDispatcher {
    public MessageDispatcher(int brokerId, SharedState sharedState, HeartbeatManager heartbeatManager, LogManager logManager, NetworkManager networkManager, ElectionInfo electionInfo) {
        MessageDispatcher.brokerId = brokerId;
        MessageDispatcher.sharedState = sharedState;
        MessageDispatcher.heartbeatManager = heartbeatManager;
        MessageDispatcher.logManager = logManager;
        MessageDispatcher.networkManager = networkManager;
        MessageDispatcher.electionInfo = electionInfo;
        pendingRequestIds = new ConcurrentLinkedQueue<>();
        pendingRequests = new ConcurrentHashMap<>();
        delayedMessages = new ConcurrentLinkedQueue<>();
    }

    private static int brokerId = 0;
    private static SharedState sharedState = null;
    private static HeartbeatManager heartbeatManager = null;
    private static LogManager logManager = null;
    private static NetworkManager networkManager = null;

    private static ElectionInfo electionInfo = null;

    private static Queue<String> pendingRequestIds = null;
    private static Map<String, Message> pendingRequests = Map.of();
    private static Queue<Message> delayedMessages = null;

    /// Dispatch the given [Message] to its dedicated handler method.
    public static void dispatch(Message message) throws ClassNotFoundException {
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
            case RequestCompletedAck msg -> handleRequestCompletedAck(msg);
            default -> throw new ClassNotFoundException();
        }
    }

    /// Returns whether the broker is the current leader of the system or not.
    private static boolean isLeader() {
        return sharedState.getLeaderId() == brokerId;
    }

    /// Returns whether the given message is related to the election of a leader or not.
    private static boolean isElectionRelated(Message msg) {
        return msg instanceof NewLeaderNomination || msg instanceof NewLeaderNominationAck || msg instanceof NewLeaderAnnouncement;
    }

    /// Returns true if the second candidate provided is better than the first one, false otherwise.
    private static boolean compareCandidates(int id1, int logLength1, int id2, int logLength2) {
        if (logLength1 > logLength2) {
            return false;
        } else if (logLength1 < logLength2) {
            return true;
        } else {
            return id2 > id1;
        }
    }

    /// Handler method for received [EntryPropagation] messages.
    private static void handleEntryPropagation(EntryPropagation msg) {
        if (!isLeader()) {
            System.out.println("[INFO]: Received propagation of log entry" + msg.logEntry() + ". Adding it to the waiting queue...");

            // store the entry as waiting for commit
            sharedState.addWaitingCommitEntry(msg.logEntry());

            // send an ACK to the leader
            Address leaderAddr = sharedState.getBrokerAddress(sharedState.getLeaderId());
            networkManager.sendMessage(new EntryPropagationAck(msg.logEntry()), leaderAddr);
        }
    }

    /// Handler method for received [EntryPropagationAck] messages.
    private static void handleEntryPropagationAck(EntryPropagationAck msg) {
        if (isLeader()) {
            if(sharedState.isEntryWaitingAck(msg.logEntry())) {
                System.out.println("[INFO]: Received propagation ACK for waiting log entry " + msg.logEntry() + ". Adding it to the log...");

                // commit the entry locally
                logManager.commitEntry(msg.logEntry());

                // broadcast the EntryCommit message
                networkManager.broadcastMessage(new EntryCommit(msg.logEntry()), brokerId, sharedState);
            }
        }
    }

    /// Handler method for received [EntryCommit] messages.
    private static void handleEntryCommit(EntryCommit msg) {
        if (!isLeader()) {
            System.out.println("[INFO]: Received commit of log entry" + msg.logEntry() + ". Adding it to the log...");

            // commit the entry locally
            logManager.commitEntry(msg.logEntry());
        }
    }

    /// Handler method for received messages that doesn't need custom handler.
    /// (They will be propagated and wait for an ack from another node).
    private static void handleGenericRequest(Message msg) {
        System.out.println("[INFO]: Received " + msg);
        if (isLeader()) {
            // create a new log entry for given request
            LogEntry newEntry = new LogEntry(sharedState.getLogLength(), msg);

            // if multi-broker system, store the entry as waiting for ACK and propagate it
            if (sharedState.getBrokersCount() > 1) {
                sharedState.addWaitingAckEntry(newEntry);
                networkManager.broadcastMessage(new EntryPropagation(newEntry), brokerId, sharedState);
            }
            // else (single-broker system) commit the entry locally
            else {
                logManager.commitEntry(newEntry);
            }
        }
        else {
            String id = null;
            if (!(msg instanceof BrokerJoinRequest)) {
                switch (msg) {
                    case ClientIdRequest req ->  id = req.clientAddress() + ":" + "-1";
                    case ClientOffsetsUpdateRequest req -> id = req.clientAddress() + ":" + req.operationId();
                    case WriteRequest req ->  id = req.clientAddress() + ":" + req.operationId();
                    default ->  throw new RuntimeException("Unexpected message type treated as Generic Request");
                }
                if (pendingRequests.putIfAbsent(id, msg) == null) {
                    pendingRequestIds.add(id);
                }

            }

            try {
                // Forward to leader
                networkManager.forwardMessageToLeader(msg, sharedState);
            } catch (RuntimeException e) {
                System.out.println("[INFO]: Delaying the message to be handled after the election.");
                if (id != null) {
                    pendingRequestIds.remove(id);
                    pendingRequests.remove(id);
                }
                delayedMessages.add(msg);  //TODO: is it right that delayed messages should be something separate?
            }
        }
    }

    /// Handler for received [ReadRequest] messages.
    private static void handleReadRequest(ReadRequest msg) {
        System.out.println("[INFO]: Received read request " + msg);

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
        networkManager.sendMessage(new ReadResponse(msg.clientId(), msg.operationId(), valuesToReturn), msg.clientAddress());

        // create a new ClientOffsetUpdate message and handle it accordingly
        ClientOffsetsUpdateRequest offsetsUpdateMsg = new ClientOffsetsUpdateRequest(msg.queueName(), requestedQueue.size(), msg.clientId(), msg.operationId(), msg.clientAddress());
        handleGenericRequest(offsetsUpdateMsg);
    }

    /// Handler for received [BrokerRemoval] messages.
    private static void handleBrokerRemoval(BrokerRemoval msg) {
        // Only process if we're not the leader (leader already removed the broker before broadcasting the BrokerRemoval message)
        if (!isLeader()) {
            System.out.println("[INFO]: Received broker removal notification for broker " + msg.brokerId() + ". Removing...");
            sharedState.removeBrokerAddress(msg.brokerId());
        }
    }

    /// Handler for received [Heartbeat] messages.
    private static void handleHeartbeat(Heartbeat msg) {
        // First check if an election is in progress
        if (electionInfo.isElectionInProgress()) {
            System.out.println("[INFO]: Ignoring heartbeat since an election is in progress");
            return;
        }

        if (!isLeader()) {
            System.out.println("[INFO]: Received heartbeat from leader");

            // Update timestamp in shared state
            heartbeatManager.updateLastHeartbeatReceived();
            // Offload ack so it isn’t blocked by other message handling
            heartbeatManager.sendAck(brokerId);


        }
    }

    /// Handler for received [HeartbeatAck] messages.
    private static void handleHeartbeatAck(HeartbeatAck msg) {
        if (isLeader()) {
            Integer followerId = msg.brokerId();
            heartbeatManager.updateLastHeartbeatAckReceived(followerId);
            System.out.println("[INFO]: Received heartbeat acknowledgment from broker " + followerId);
        }
    }

    /// Handler for received [NewLeaderNomination] messages.
    private static void handleNewLeaderNomination(NewLeaderNomination msg) {
        System.out.println("[INFO]: Received leader nomination from broker " + msg.brokerId() + " with log length " + msg.logLength());

        // If this is the first nomination received...
        if (electionInfo.wasntElectionInProgress()) {
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
                networkManager.sendMessage(new NewLeaderNominationAck(brokerId), senderAddr);
            }
            // else...
            else {
                System.out.println("[INFO]: I'm better, will broadcast nomination");

                // set self as best candidate and broadcast nomination
                electionInfo.updateBestCandidate(brokerId, myLogLength);
                networkManager.broadcastMessage(new NewLeaderNomination(brokerId, myLogLength), brokerId, sharedState);

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
                networkManager.sendMessage(new NewLeaderNominationAck(brokerId), senderAddr);
            }
            // else, simply ignore the nomination
            else {
                System.out.println("[INFO]: stored best is better, ignoring received nomination");
            }
        }
    }

    /// Handler for received [NewLeaderNominationAck] messages.
    private static void handleNewLeaderNominationAck(NewLeaderNominationAck msg) {
        // if an election is in progress and I'm the best candidate...
        if (electionInfo.isElectionInProgress() && electionInfo.getBestCandidate() == brokerId) {
            // add the id of the sender to the set of received ACKs
            electionInfo.addReceivedAck(msg.senderId());

            System.out.println("[INFO]: Received nomination ACK from broker " + msg.senderId());

            int receivedAcksCount = electionInfo.getReceivedAcksCount();
            int activeBrokersCount = sharedState.getBrokersCount();


            System.out.println("[INFO]: Current ACK count: " + receivedAcksCount + "/" + activeBrokersCount);

            // if all ACKs have been received...
            if (receivedAcksCount >= activeBrokersCount - 1) {
                System.out.println("[INFO]: Received ACKs from all " + activeBrokersCount + " active brokers. Becoming new leader");

                // become leader
                sharedState.setNewLeaderId(brokerId);

                // broadcast new leader announcement
                List<LogEntry> completeLog = sharedState.getCompleteLog();
                networkManager.broadcastMessage(new NewLeaderAnnouncement(brokerId, completeLog), brokerId, sharedState);

                // terminate the election phase
                electionInfo.stopElection();

                // restart the heartbeat manager
                heartbeatManager.restart();
                resendPendingRequests();
                processDelayedMessages();
            }
        }
    }

    /// Handler for received [NewLeaderAnnouncement] messages.
    private static void handleNewLeaderAnnouncement(NewLeaderAnnouncement msg) {
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
            heartbeatManager.restart();
            resendPendingRequests();
            processDelayedMessages();
        }
    }

    private static void handleRequestCompletedAck(RequestCompletedAck msg) {
        String id = msg.clientAddress() + ":" + msg.operationId();
        pendingRequestIds.remove(id);
        pendingRequests.remove(id);
        System.out.println("[INFO]: Completed request " + id + " has been removed from pending requests");
    }

    public static void resendPendingRequests() {
        while (!pendingRequestIds.isEmpty()) {
            String id = pendingRequestIds.poll();
            Message msg = pendingRequests.get(id);

            try {
                dispatch(msg);
            } catch (ClassNotFoundException e) {
                System.out.println("[ERROR]: dispatch failed for pending id=" + id + ": " + e.getMessage());
            }
        }
    }

    public static void processDelayedMessages() {
        while (!delayedMessages.isEmpty()) {
            Message msg = delayedMessages.poll();
            try {
                dispatch(msg);
            } catch (ClassNotFoundException e) {
                System.out.println("[ERROR]: dispatch failed for delayed msg=" + msg + ": " + e.getMessage());
            }
        }
    }
}
