package it.polimi.ds.reliable_queuing_system.messages;

import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

/// ACK message sent by followers in reply to [EntryPropagation].
public record EntryPropagationAck(
        LogEntry logEntry
) implements Message {
}
