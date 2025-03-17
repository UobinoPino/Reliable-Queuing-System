package it.polimi.ds.reliable_queuing_system.messages;

import it.polimi.ds.reliable_queuing_system.utils.Address;

/// Message sent from a client to any broker to read the new values from the given queue.
public record ReadRequest(
        String queueName,
        int clientId,
        int operationId,
        Address clientAddress
) implements Message {
}
