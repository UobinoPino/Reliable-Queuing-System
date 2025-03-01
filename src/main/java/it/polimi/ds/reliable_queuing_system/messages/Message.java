package it.polimi.ds.reliable_queuing_system.messages;

import java.io.Serializable;

/// An interface implemented by all the possible messages shared by the application over TCP Sockets.
public sealed interface Message extends Serializable permits BrokerAddition, BrokerAdditionAck, BrokerJoinRequest, BrokerJoinResponse, BrokerRemoval, ClientIdAssignment, ClientIdRequest, ClientOffsetsUpdate, EntryCommit, EntryPropagation, EntryPropagationAck, Heartbeat, HeartbeatAck, NewLeaderAnnouncement, NewLeaderNomination, NewLeaderNominationAck, ReadConfirmation, ReadRequest, ReadResponse, WriteRequest, WriteResponse
{
}
