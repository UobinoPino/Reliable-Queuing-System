package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.*;
import it.polimi.ds.reliable_queuing_system.utils.Address;
import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/// A class that will take care of handling messages received by the broker.
public class MessageDispatcher {
    public MessageDispatcher(int brokerId, SharedState sharedState, HeartbeatManager heartbeatManager, LogManager logManager, NetworkManager networkManager, ElectionInfo electionInfo) {
        this.brokerId = brokerId;
        this.sharedState = sharedState;
        this.heartbeatManager = heartbeatManager;
        this.logManager = logManager;
        this.networkManager = networkManager;
        this.electionInfo = electionInfo;
        this.pendingRequestIds = new ConcurrentLinkedQueue<>();
        this.pendingRequests = new ConcurrentHashMap<>();
        this.delayedMessages = new ConcurrentLinkedQueue<>();

        electionInfo.addPostElectionCallback(this::resendPendingRequests);
        electionInfo.addPostElectionCallback(this::processDelayedMessages);
    }

    private final int brokerId;
    private final SharedState sharedState;
    private final HeartbeatManager heartbeatManager;
    private final LogManager logManager;
    private final NetworkManager networkManager;

    private final ElectionInfo electionInfo;

    private final Queue<String> pendingRequestIds;
    private final Map<String, Message> pendingRequests;
    private final Queue<Message> delayedMessages;
    private final ScheduledExecutorService batchProcessingExecutor = Executors.newSingleThreadScheduledExecutor();
    private static final int BATCH_SIZE = 20; // Process only 20 messages at a time

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
            case RequestCompletedAck msg -> handleRequestCompletedAck(msg);
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
            System.out.println("[INFO]: Received propagation of log entry" + msg.logEntry() + ". Adding it to the waiting queue...");

            // store the entry as waiting for commit
            sharedState.addWaitingCommitEntry(msg.logEntry());

            // send an ACK to the leader
            Address leaderAddr = sharedState.getBrokerAddress(sharedState.getLeaderId());
            networkManager.sendMessage(new EntryPropagationAck(msg.logEntry()), leaderAddr);
        }
    }

    /// Handler method for received [EntryPropagationAck] messages.
    private void handleEntryPropagationAck(EntryPropagationAck msg) {
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
    private void handleEntryCommit(EntryCommit msg) {
        if (!isLeader()) {
            System.out.println("[INFO]: Received commit of log entry" + msg.logEntry() + ". Adding it to the log...");

            // commit the entry locally
            logManager.commitEntry(msg.logEntry());
        }
    }

    /// Handler method for received messages that doesn't need custom handler.
    /// (They will be propagated and wait for an ack from another node).
    private void handleGenericRequest(Message msg) {
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
                    case ClientIdRequest req -> id = req.clientAddress() + ":" + "-1";
                    case ClientOffsetsUpdateRequest req -> id = req.clientAddress() + ":" + req.operationId();
                    case WriteRequest req -> id = req.clientAddress() + ":" + req.operationId();
                    default -> throw new RuntimeException("Unexpected message type treated as Generic Request");
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
                delayedMessages.add(msg);
            }
        }
    }

    /// Handler for received [ReadRequest] messages.
    private void handleReadRequest(ReadRequest msg) {
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
    private void handleBrokerRemoval(BrokerRemoval msg) {
        // Only process if we're not the leader (leader already removed the broker before broadcasting the BrokerRemoval message)
        if (!isLeader()) {
            System.out.println("[INFO]: Received broker removal notification for broker " + msg.brokerId() + ". Removing...");
            sharedState.removeBrokerAddress(msg.brokerId());
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
            // Offload ack so it isn’t blocked by other message handling
            heartbeatManager.sendAck(brokerId);


        }
    }

    /// Handler for received [HeartbeatAck] messages.
    private void handleHeartbeatAck(HeartbeatAck msg) {
        if (isLeader()) {
            Integer followerId = msg.brokerId();
            heartbeatManager.updateLastHeartbeatAckReceived(followerId);
            System.out.println("[INFO]: Received heartbeat acknowledgment from broker " + followerId);
        }
    }

    /// Handler for received [NewLeaderNomination] messages.
    private void handleNewLeaderNomination(NewLeaderNomination msg) {
        System.out.println("[INFO]: Received leader nomination from broker " + msg.brokerId() + " with log length " + msg.logLength());

        // If this is the first nomination received...
        if (electionInfo.wasntElectionInProgress()) {
            // stop the heartbeat manager
            heartbeatManager.stop();

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
                boolean success = networkManager.sendMessage(new NewLeaderNominationAck(brokerId), senderAddr);
                if (!success) {
                    System.out.println("[INFO]: Restoring previous best candidate");

                    electionInfo.updateBestCandidate(brokerId, myLogLength);

                    System.out.println("[INFO]: Previous best candidate was self, re-broadcasting nomination");
                    electionInfo.startNominationAckTimeouts(sharedState.getBrokerAddresses().keySet(), brokerId, sharedState, networkManager, heartbeatManager);
                    networkManager.broadcastMessage(new NewLeaderNomination(brokerId, myLogLength), brokerId, sharedState);

                }
            }
            // else...
            else {
                System.out.println("[INFO]: I'm better, will broadcast nomination");

                // set self as best candidate and broadcast nomination
                electionInfo.updateBestCandidate(brokerId, myLogLength);
                electionInfo.startNominationAckTimeouts(sharedState.getBrokerAddresses().keySet(), brokerId, sharedState, networkManager, heartbeatManager);
                networkManager.broadcastMessage(new NewLeaderNomination(brokerId, myLogLength), brokerId, sharedState);


            }
        }
        // else (an election was already in progress)...
        else {
            System.out.println("[INFO]: Comparing log lengths - stored best ("+ electionInfo.getBestCandidate() +"): " + electionInfo.getBestCandidateLogLength() + ", candidate (" + msg.brokerId() + "): " + msg.logLength());

            // compared received log with the stored best one
            int bestCandidateId = electionInfo.getBestCandidate();
            int bestCandidateLogLength = electionInfo.getBestCandidateLogLength();
            boolean candidateIsBetter = compareCandidates(bestCandidateId, bestCandidateLogLength, msg.brokerId(), msg.logLength());

            // if received is better, update best accordingly and send ACK
            if (candidateIsBetter) {
                System.out.println("[INFO]: Candidate is better, will send ACK");

                electionInfo.updateBestCandidate(msg.brokerId(), msg.logLength());

                Address senderAddr = sharedState.getBrokerAddress(msg.brokerId());
                boolean success = networkManager.sendMessage(new NewLeaderNominationAck(brokerId), senderAddr);
                if (!success) {
                    System.out.println("[INFO]: Restoring previous best candidate");

                    electionInfo.updateBestCandidate(bestCandidateId, bestCandidateLogLength);

                    // and if it was self, re-broadcast nomination
                    if (bestCandidateId == brokerId) {
                        electionInfo.startNominationAckTimeouts(sharedState.getBrokerAddresses().keySet(), brokerId, sharedState, networkManager, heartbeatManager);
                        networkManager.broadcastMessage(new NewLeaderNomination(bestCandidateId, bestCandidateLogLength), brokerId, sharedState);

                    }
                }
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


            System.out.println("[INFO]: Current ACK count: " + receivedAcksCount + "/" + (activeBrokersCount-1));

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
                electionInfo.executePostElectionCallbacks();
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
            heartbeatManager.restart();
            electionInfo.executePostElectionCallbacks();
        }
    }

    private void handleRequestCompletedAck(RequestCompletedAck msg) {
        String id = msg.clientAddress() + ":" + msg.operationId();
        pendingRequestIds.remove(id);
        pendingRequests.remove(id);
        System.out.println("[INFO]: Completed request " + id + " has been removed from pending requests");
    }

    private void resendPendingRequests() {
        // Schedule gradual processing of pending requests
        if (!pendingRequestIds.isEmpty()) {
            System.out.println("[INFO]: Scheduling processing of " + pendingRequestIds.size() + " pending requests in batches");
            batchProcessingExecutor.schedule(this::processBatch, 250, TimeUnit.MILLISECONDS);
        }
    }

    private void processDelayedMessages() {
        // This will be handled by the batch processing
        System.out.println("[INFO]: Delayed messages (" + delayedMessages.size() + ") will be processed in batches");
    }

    private void processBatch() {
        int processedCount = 0;
        boolean hasMore = false;

        // Process a batch of pending requests
        while (!pendingRequestIds.isEmpty() && processedCount < BATCH_SIZE) {
            String id = pendingRequestIds.poll();
            Message msg = pendingRequests.get(id);

            try {
                dispatch(msg);
                processedCount++;
            } catch (ClassNotFoundException e) {
                System.out.println("[ERROR]: dispatch failed for pending id=" + id + ": " + e.getMessage());
            }
            hasMore = !pendingRequestIds.isEmpty();
        }

        // If batch size not reached and there are delayed messages, process some
        while (!delayedMessages.isEmpty() && processedCount < BATCH_SIZE) {
            Message msg = delayedMessages.poll();
            try {
                dispatch(msg);
                processedCount++;
            } catch (ClassNotFoundException e) {
                System.out.println("[ERROR]: dispatch failed for delayed msg=" + msg + ": " + e.getMessage());
            }
            hasMore = hasMore || !delayedMessages.isEmpty();
        }

        // If more messages to process, schedule another batch after a delay
        if (hasMore) {
            System.out.println("[INFO]: Scheduled next batch of messages. Remaining pending: " +
                              pendingRequestIds.size() + ", delayed: " + delayedMessages.size());
            batchProcessingExecutor.schedule(this::processBatch, 500, TimeUnit.MILLISECONDS);
        }
    }
}
