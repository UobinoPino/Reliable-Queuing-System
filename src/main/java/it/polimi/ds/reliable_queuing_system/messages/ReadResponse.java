package it.polimi.ds.reliable_queuing_system.messages;

import java.util.List;

/// Message sent from a follower to a client as a reply to [ReadRequest], containing the requested
/// values which should not be used immediately since the client offsets haven't been updated yet,
/// but instead should be used after having received a [ReadConfirmation].
public record ReadResponse(
        int clientId,
        int operationId,
        List<Integer> values
) implements Message {
}
