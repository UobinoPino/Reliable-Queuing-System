package it.polimi.ds.reliable_queuing_system.messages;

/// ACK message sent by brokers in reply to [NewLeaderNomination] only if
/// they accept the sender as new leader.
public record NewLeaderNominationAck(int senderId) implements Message {
}
