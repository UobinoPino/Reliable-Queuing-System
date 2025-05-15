package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.*;
import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

/// A class that will take care of committing log entries.
public class LogManager {
    public LogManager(int brokerId, SharedState sharedState, NetworkManager networkManager) {
        this.brokerId = brokerId;
        this.sharedState = sharedState;
        this.networkManager = networkManager;
    }

    private final int brokerId;
    private final SharedState sharedState;

    private final NetworkManager networkManager;

    /// Tries to commit the given [LogEntry] in the [SharedState] log
    /// and (if succeeded) perform the operation associated with it.
    public void commitEntry(LogEntry entry) {
        boolean committed = sharedState.commitEntry(entry);

        if (committed) {
            performEntryOperation(entry);
        }
    }

    /// Perform the operation corresponding to the given [LogEntry]
    private void performEntryOperation(LogEntry entry) {
        Message msg = entry.message();
        switch (msg) {
            case BrokerJoinRequest m -> addBroker(m);
            case ClientIdRequest m -> assignClientId(m);
            case ClientOffsetsUpdateRequest m -> updateClientOffset(m);
            case WriteRequest m -> writeValue(m);
            default -> throw new IllegalStateException("Unexpected value: " + msg);
        }
    }

    /// Returns whether the broker is the current leader of the system or not.
    private boolean isLeader() {
        return sharedState.getLeaderId() == brokerId;
    }

    /// Add to the list of known brokers in the [SharedState] the broker who sent the given [BrokerJoinRequest].
    private void addBroker(BrokerJoinRequest req) {
        // obtain a new id for the broker to be added
        int newBrokerId = sharedState.getNewBrokerId();

        // Add the broker to the list in the shared state
        sharedState.addBrokerAddress(newBrokerId, req.brokerAddress());

        // If leader, return a BrokerJoinResponse to the requesting broker
        if (isLeader()) {
            networkManager.sendMessage(new BrokerJoinResponse(newBrokerId, sharedState), req.brokerAddress());
        }
    }

    /// Assign the next available id to the client who sent the given [ClientIdRequest].
    private void assignClientId(ClientIdRequest req) {
        // get a new client id from the shared state
        int clientId = sharedState.getClientId(req.clientAddress());

        // if leader, return this id to the requesting client
        if (isLeader()) {
            networkManager.sendMessage(new ClientIdAssignment(clientId), req.clientAddress());
        }
    }

    /// Update the client offsets stored in the [SharedState] according to the given [ClientOffsetsUpdateRequest].
    private void updateClientOffset(ClientOffsetsUpdateRequest req) {
        // update the offsets
        sharedState.updateClientOffset(req.clientId(), req.queueName(), req.newOffset());

        // if leader, return the read confirmation to the requesting client
        if(isLeader()){
            networkManager.sendMessage(new ReadConfirmation(req.operationId()), req.clientAddress());
        }
    }

    /// Update the queues in the [SharedState] according to the given [WriteRequest].
    private void writeValue(WriteRequest req) {
        // write the new value in the queue
        sharedState.addToQueue(req.queueName(), req.value());

        // if leader, send the WriteResponse to the requesting client
        if(isLeader()){
            networkManager.sendMessage(new WriteResponse(req.operationId()), req.clientAddress());
        }
    }
}
