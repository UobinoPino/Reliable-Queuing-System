package it.polimi.ds.reliable_queuing_system.messages;

import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

import java.util.List;

/// Message sent by a broker after all the expected [NewLeaderNominationAck] has been received
/// to announce itself as the new leader.
public record NewLeaderAnnouncement(
        int brokerId,
        List<LogEntry> newLeaderLog
) implements Message {
}
