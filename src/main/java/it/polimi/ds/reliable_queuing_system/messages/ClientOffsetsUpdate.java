package it.polimi.ds.reliable_queuing_system.messages;

/// Message sent by a follower to the leader after a new read operation has been performed
/// by a client and therefore its offsets have been updated.
public record ClientOffsetsUpdate(
        String queueId,
        int clientId,
        int operationId
) implements Message {
}
