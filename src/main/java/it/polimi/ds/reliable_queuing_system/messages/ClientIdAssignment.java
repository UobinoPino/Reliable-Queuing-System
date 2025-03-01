package it.polimi.ds.reliable_queuing_system.messages;

/// Message sent from the leader to the client (eventually through the follower that
/// originally received the request) in reply to [ClientIdRequest].
public record ClientIdAssignment(
        int clientId
) implements Message {
}
