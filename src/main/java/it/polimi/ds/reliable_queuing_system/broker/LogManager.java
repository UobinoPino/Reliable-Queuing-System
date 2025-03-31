package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.*;
import it.polimi.ds.reliable_queuing_system.utils.Address;
import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

import java.util.Map;

/// A class that will take care of committing log entries.
public class LogManager {
    public LogManager(int brokerId, SharedState sharedState) {
        this.brokerId = brokerId;
        this.sharedState = sharedState;
    }

    private final int brokerId;
    private final SharedState sharedState;

    /// Commit the given [LogEntry] in the [SharedState] log and perform the
    /// operation associated with it.
    public void commitEntry(LogEntry entry) {
        sharedState.commitEntry(entry);

        performEntryOperation(entry);
    }

    /// Perform the operation corresponding to the given [LogEntry]
    private void performEntryOperation(LogEntry entry) {
        Message msg = entry.message();
        switch (msg) {
            case BrokerJoinRequest m -> addBroker(m);
            case ClientIdRequest m -> assignClientId(m);
            case ClientOffsetsUpdateRequest m -> updateClientOffset(m);
            case WriteRequest m -> writeValue(m);
            default -> throw new IllegalStateException("Unexpected value: " + msg);  //TODO: replace with proper error handling
        }
    }

    /// Returns whether the broker is the current leader of the system or not.
    private boolean isLeader() {
        return sharedState.getLeaderId() == brokerId;
    }

    /// Add to the list of known brokers in the [SharedState] the broker who sent the given [BrokerJoinRequest].
    private void addBroker(BrokerJoinRequest req) {
        //FIXME: the check for existance of the address below was a temporary fix for the
        // problem of new followers adding themselves twice. Maybe not needed when that error will be fixed?

        // Check if the address already exists in the system
        boolean brokerAlreadyPresent = false;
        Map<Integer, Address> existingBrokers = sharedState.getBrokerAddresses();
        for (Map.Entry<Integer, Address> entry : existingBrokers.entrySet()) {
            Address existingAddr = entry.getValue();
            if (existingAddr.ip().equals(req.brokerAddress().ip()) &&
                    existingAddr.port().equals(req.brokerAddress().port())) {
                // Address already exists, use the existing broker ID
                brokerAlreadyPresent = true;
                System.out.println("[INFO]: Broker with address " + req.brokerAddress() +
                        " already exists with ID " + entry.getKey());
                break;
            }
        }

        // If no existing broker with this address, get a new broker ID
        if (!brokerAlreadyPresent) {
            int newBrokerId = sharedState.getNewBrokerId();
            // Add the broker to the list in the shared state
            sharedState.addBrokerAddress(newBrokerId, req.brokerAddress());

            // If leader, return a BrokerJoinResponse to the requesting broker
            if (isLeader()) {
                NetworkManager.sendMessage(new BrokerJoinResponse(newBrokerId, sharedState), req.brokerAddress());
            }
        }
    }

    /// Assign the next available id to the client who sent the given [ClientIdRequest].
    private void assignClientId(ClientIdRequest req) {
        // get a new client id from the shared state
        int clientId = sharedState.getNewClientId();

        // if leader, return this id to the requesting client
        if (isLeader()) {
            NetworkManager.sendMessage(new ClientIdAssignment(clientId), req.clientAddress());
        }
    }

    /// Update the client offsets stored in the [SharedState] according to the given [ClientOffsetsUpdateRequest].
    private void updateClientOffset(ClientOffsetsUpdateRequest req) {
        // update the offsets
        sharedState.updateClientOffset(req.clientId(), req.queueName(), req.newOffset());

        // if leader, return the read confirmation to the requesting client
        if(isLeader()){
            NetworkManager.sendMessage(new ReadConfirmation(req.operationId()), req.clientAddress());
        }
    }

    /// Update the queues in the [SharedState] according to the given [WriteRequest].
    private void writeValue(WriteRequest req) {
        // write the new value in the queue
        sharedState.addToQueue(req.queueName(), req.value());

        // if leader, send the WriteResponse to the requesting client
        if(isLeader()){
            NetworkManager.sendMessage(new WriteResponse(req.operationId()), req.clientAddress());
        }
    }
}
