package it.polimi.ds.reliable_queuing_system.messages;

/// Message sent by a broker after the leader has crashed to candidate itself as new leader.
public record NewLeaderNomination(
        int currentEpoch,
        int brokerId,
        int logLength
) implements Message {
}
