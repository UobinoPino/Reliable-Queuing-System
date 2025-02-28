package it.polimi.ds.reliable_queuing_system.messages;

/// Message sent by the leader to the followers to notify that a new broker joined the system.
public record BrokerAddition(
        String brokerIp,
        int brokerPort,
        int brokerId
) implements Message {
}
