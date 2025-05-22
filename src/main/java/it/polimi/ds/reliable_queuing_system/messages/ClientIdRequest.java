package it.polimi.ds.reliable_queuing_system.messages;

import it.polimi.ds.reliable_queuing_system.utils.Address;

/// Message sent from a client to the leader (or to any follower that will forward it to
/// the leader) to obtain a unique id.
public record ClientIdRequest(
        Address clientAddress,
        Address contactedBrokerAddress
) implements Message {
}
