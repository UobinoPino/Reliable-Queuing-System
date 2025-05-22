package it.polimi.ds.reliable_queuing_system.messages;

import java.util.List;

/// Message sent from a follower to a client as a reply to [ReadRequest], containing the requested values.
public record ReadResponse(
        int clientId,
        int operationId,
        String queueName,
        List<Integer> values
) implements Message {
}
