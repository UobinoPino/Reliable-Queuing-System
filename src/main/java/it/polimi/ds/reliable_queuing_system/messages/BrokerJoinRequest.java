package it.polimi.ds.reliable_queuing_system.messages;

import it.polimi.ds.reliable_queuing_system.utils.Address;

/// Message sent from a new broker that wants to join the system to the leader
/// (or to any follower that will then forward it to the leader).
public record BrokerJoinRequest(
        Address brokerAddress
) implements Message {
}
