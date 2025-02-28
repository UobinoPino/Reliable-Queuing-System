package it.polimi.ds.reliable_queuing_system.messages;

/// Message sent from the leader to the followers to propagate a new log entry
/// (that will not be added to the log until the corresponding [EntryCommit] will
/// be received).
public record EntryPropagation(
        Message logEntry,
        int logIndex
) implements Message {
}
