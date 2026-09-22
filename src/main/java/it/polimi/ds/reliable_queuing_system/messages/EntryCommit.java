package it.polimi.ds.reliable_queuing_system.messages;

import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

/// Message sent from the leader to the followers once the entry has been replicated on a
/// **majority** of the brokers (i.e. once the leader collected `floor(n/2)` distinct
/// [EntryPropagationAck] messages, which together with the leader's own copy make
/// `floor(n/2) + 1` replicas), to confirm the addition of the given entry to the shared log.
public record EntryCommit(
        LogEntry logEntry
) implements Message {
}