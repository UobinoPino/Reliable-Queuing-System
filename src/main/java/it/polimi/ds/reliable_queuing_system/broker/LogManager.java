package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.*;
import it.polimi.ds.reliable_queuing_system.utils.Address;
import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

import java.util.ArrayList;
import java.util.List;

/// A class that will take care of committing log entries.
public class LogManager {
    public LogManager(int brokerId, Address brokerAddress, SharedState sharedState, NetworkManager networkManager) {
        this.brokerId = brokerId;
        this.brokerAddress = brokerAddress;
        this.sharedState = sharedState;
        this.networkManager = networkManager;
    }

    private final int brokerId;
    private final Address brokerAddress;
    private final SharedState sharedState;

    private final NetworkManager networkManager;

    /// Tries to commit the given [LogEntry] in the [SharedState] log
    /// and (if succeeded) perform the operation associated with it.
    /// Returns whether the commit was successful or not.
    public boolean commitEntry(LogEntry entry) {
        boolean committed = sharedState.commitEntry(entry);

        if (committed) {
            performEntryOperation(entry);
        }

        return committed;
    }

    /// Perform the operation corresponding to the given [LogEntry]
    private void performEntryOperation(LogEntry entry) {
        Message msg = entry.message();
        switch (msg) {
            case BrokerJoinRequest m -> addBroker(m);
            case ClientIdRequest m -> assignClientId(m);
            case ReadRequest m -> readValues(m);
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

    /// Assign the next available id to the client who sent the given [ClientIdRequest] and, if the request was
    /// originally sent by the client to this broker, also sends back a [ClientIdAssignment].
    private void assignClientId(ClientIdRequest req) {
        // get a new client id from the shared state
        int clientId = sharedState.getClientId(req.clientAddress());

        // if I'm the broker originally contacted by the client, return this id to the requesting client
        if (req.contactedBrokerAddress().equals(brokerAddress)) {
            networkManager.sendMessage(new ClientIdAssignment(clientId), req.clientAddress());
        }
    }

    /// Update the client offsets stored in the [SharedState] according to the given [ClientIdRequest] and, if the
    /// request was originally sent by the client to this broker, also sends back a [ReadResponse] with the read values
    /// to the client.
    private void readValues(ReadRequest req) {
        // Retrieve the values from the queue
        List<Integer> newValues = new ArrayList<>();
        List<Integer> requestedQueue = sharedState.getQueue(req.queueName());
        if (!requestedQueue.isEmpty()) {
            int oldOffset = sharedState.getClientOffset(req.clientId(), req.queueName());
            if (oldOffset < requestedQueue.size()) {
                newValues.addAll(requestedQueue.subList(oldOffset, requestedQueue.size()));
            }
        }

        // Update the client offset
        sharedState.updateClientOffset(req.clientId(), req.queueName(), requestedQueue.size());

        // if I'm the broker originally contacted by the client, return the read confirmation to the requesting client
        if(req.contactedBrokerAddress().equals(brokerAddress)){
            networkManager.sendMessage(new ReadResponse(req.clientId(), req.operationId(), req.queueName(), newValues), req.clientAddress());
            System.out.println("Sending back results for op " + req.operationId() + " to " + req.clientAddress());
        }
    }

    /// Update the queues in the [SharedState] according to the given [WriteRequest] and, if the
    /// request was originally sent by the client to this broker, also send back a [WriteResponse] to the client.
    private void writeValue(WriteRequest req) {
        // write the new value in the queue
        sharedState.addToQueue(req.queueName(), req.value());

        // if I'm the broker originally contacted by the client, send the WriteResponse to the requesting client
        if(req.contactedBrokerAddress().equals(brokerAddress)){
            networkManager.sendMessage(new WriteResponse(req.operationId()), req.clientAddress());
            System.out.println("Sending back results for op " + req.operationId() + " to " + req.clientAddress());
        }
    }
}
