package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.*;
import it.polimi.ds.reliable_queuing_system.utils.Address;
import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

import java.util.ArrayList;
import java.util.List;

public class MessageDispatcher {
    public MessageDispatcher(int brokerId, SharedState sharedState, HeartbeatManager heartbeatManager, LogManager logManager) {
        this.brokerId = brokerId;
        this.sharedState = sharedState;
        this.heartbeatManager = heartbeatManager;
        this.logManager = logManager;
    }

    private final int brokerId;
    private final SharedState sharedState;
    private final HeartbeatManager heartbeatManager;
    private final LogManager logManager;

    /// Dispatch the given [Message] to its dedicated handler method.
    public void dispatch(Message message) throws ClassNotFoundException {
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

    private void handleEntryPropagation(EntryPropagation msg) {
        if (!isLeader()) {
            // store the entry as waiting for commit
            sharedState.addWaitingCommitEntry(msg.logEntry());

            // send an ACK to the leader
            Address leaderAddr = sharedState.getBrokerAddress(sharedState.getLeaderId());
            NetworkManager.sendMessage(new EntryPropagationAck(msg.logEntry()), leaderAddr);
        }
    }

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

    private void handleEntryCommit(EntryCommit msg) {
        if (!isLeader()) {
            // commit the entry locally
            logManager.commitEntry(msg.logEntry());
        }
    }

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

    private void handleBrokerRemoval(BrokerRemoval msg) {
        //TODO: maybe broker removal should be handled using log entries as well? Otherwise it wouldn't appear in the log...

        // Only process if we're not the leader (leader already removed the broker before broadcasting the BrokerRemoval message)
        if (!isLeader()) {
            sharedState.removeBrokerAddress(msg.brokerId());
            System.out.println("[INFO]: Follower " + brokerId + ": Received broker removal notification for broker " + msg.brokerId());
        }
    }

    private void handleHeartbeat(Heartbeat msg) {
        if (!isLeader()) {
            System.out.println("[INFO]: Received heartbeat from leader");

            // Update timestamp in shared state
            heartbeatManager.updateLastHeartbeatReceived();

            // Send HeartbeatAck to the leader
            NetworkManager.forwardMessageToLeader(new HeartbeatAck(brokerId), brokerId, sharedState);
        }
    }

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
}
