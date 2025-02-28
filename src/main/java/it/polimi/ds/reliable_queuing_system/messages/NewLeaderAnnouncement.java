package it.polimi.ds.reliable_queuing_system.messages;

/// Message sent by a broker after all the expected [NewLeaderNominationAck] has been received
/// to announce itself as the new leader.
public record NewLeaderAnnouncement(
        int brokerId
) implements Message {
}
