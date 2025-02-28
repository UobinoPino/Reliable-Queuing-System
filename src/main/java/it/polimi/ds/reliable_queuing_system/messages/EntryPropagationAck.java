package it.polimi.ds.reliable_queuing_system.messages;

/// ACK message sent by followers in reply to [EntryPropagation].
public record EntryPropagationAck(
        Message logEntry,
        int logIndex
) implements Message {
}
