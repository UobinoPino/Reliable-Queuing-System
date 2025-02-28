package it.polimi.ds.reliable_queuing_system.messages;

/// Message sent from a new broker that wants to join the system to the leader
/// (or to any follower that will then forward it to the leader).
public record BrokerJoinRequest(
        String brokerIp,
        int brokerPort
) implements Message {
}
