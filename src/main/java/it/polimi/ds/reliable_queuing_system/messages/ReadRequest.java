package it.polimi.ds.reliable_queuing_system.messages;

/// Message sent from a client to any broker to read the new values from the given queue.
public record ReadRequest(
        String queueId,
        int clientId,
        int operationId
) implements Message {
}
