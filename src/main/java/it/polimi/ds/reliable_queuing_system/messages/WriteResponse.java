package it.polimi.ds.reliable_queuing_system.messages;

/// Message sent from a follower to a client to communicate that the write operation with the
/// given id has been successfully processed by the system.
public record WriteResponse(
        int operationId
) implements Message {
}
