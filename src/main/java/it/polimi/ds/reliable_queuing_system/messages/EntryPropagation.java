package it.polimi.ds.reliable_queuing_system.messages;

import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

/// Message sent from the leader to the followers to propagate a new log entry
/// (that will not be added to the log until the corresponding [EntryCommit] will
/// be received).
public record EntryPropagation(
        LogEntry logEntry
) implements Message {
}
