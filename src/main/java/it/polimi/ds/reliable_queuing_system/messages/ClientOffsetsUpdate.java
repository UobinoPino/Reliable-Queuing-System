package it.polimi.ds.reliable_queuing_system.messages;

import it.polimi.ds.reliable_queuing_system.utils.Address;

/// Message sent by a follower to the leader after a new read operation has been performed
/// by a client and therefore its offsets have been updated.
public record ClientOffsetsUpdate(
        String queueName,
        Integer newOffset,
        int clientId,
        int operationId,
        Address clientAddress
) implements Message {
}
