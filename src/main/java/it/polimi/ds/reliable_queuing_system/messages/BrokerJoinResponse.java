package it.polimi.ds.reliable_queuing_system.messages;

import it.polimi.ds.reliable_queuing_system.broker.SharedState;

/// Message sent by the leader to the newly added broker (eventually through the follower it
/// originally contacted) to confirm the entrance in the system and provide the current
/// replicated state.
public record BrokerJoinResponse(
        SharedState sharedState
) implements Message {
}
