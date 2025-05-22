package it.polimi.ds.reliable_queuing_system.messages;

import it.polimi.ds.reliable_queuing_system.utils.Address;

/// Message sent from a client to the leader (or to a follower that will then forward
/// it to the leader) to append a new value in the given queue.
public record WriteRequest(
        String queueName,
        int value,
        int clientId,
        int operationId,
        Address clientAddress,
        Address contactedBrokerAddress
) implements Message {
}
