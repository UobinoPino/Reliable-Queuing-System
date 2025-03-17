package it.polimi.ds.reliable_queuing_system.utils;

import it.polimi.ds.reliable_queuing_system.messages.Message;

import java.io.Serializable;

/// A record that represents an entry in the replicated log of the system.
public record LogEntry(
        int index,
        Message message
) implements Serializable {
}
