package it.polimi.ds.reliable_queuing_system.messages;

/// Message sent from the leader to the followers after at least one [EntryPropagationAck]
/// has been received, to confirm the addition of the given entry to the shared log.
public record EntryCommit(
        Message logEntry,
        int logIndex
) implements Message {
}
