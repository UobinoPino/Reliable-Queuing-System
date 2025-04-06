package it.polimi.ds.reliable_queuing_system.messages;

import it.polimi.ds.reliable_queuing_system.utils.LogEntry;
import java.util.List;

/// Message sent from a new leader to all followers containing its entire log that should replace their logs
public record LeaderLogSync(
        int leaderId,
        List<LogEntry> completeLog
) implements Message {
}
