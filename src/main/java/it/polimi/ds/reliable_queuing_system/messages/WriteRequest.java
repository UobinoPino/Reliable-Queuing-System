package it.polimi.ds.reliable_queuing_system.messages;

/// Message sent from a client to the leader (or to a follower that will then forward
/// it to the leader) to append a new value in the given queue.
public record WriteRequest(
        String queueId,
        int clientId,
        int operationId,
        int value
) implements Message {
}
