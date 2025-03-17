package it.polimi.ds.reliable_queuing_system.messages;

import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

/// Message sent from the leader to the followers after at least one [EntryPropagationAck]
/// has been received, to confirm the addition of the given entry to the shared log.
public record EntryCommit(
        LogEntry logEntry
) implements Message {
}
