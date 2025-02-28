package it.polimi.ds.reliable_queuing_system.messages;

/// Message sent by the leader to the followers to notify that a broker has been removed from the system.
public record BrokerRemoval(
        int brokerId
) implements Message {
}
