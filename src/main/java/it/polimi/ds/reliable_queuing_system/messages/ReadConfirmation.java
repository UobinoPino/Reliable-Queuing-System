package it.polimi.ds.reliable_queuing_system.messages;

/// Message sent from a follower to a client to confirm that the values returned by
/// [ReadResponse] can be safely used since the read offsets have been properly updated.
public record ReadConfirmation(
        int operationId
) implements Message {
}
