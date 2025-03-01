package it.polimi.ds.reliable_queuing_system.messages;

/// Message sent from a client to the leader (or to any follower that will forward it to
/// the leader) to obtain a unique id.
public record ClientIdRequest() implements Message {
}
