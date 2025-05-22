package it.polimi.ds.reliable_queuing_system.messages;

import java.io.Serializable;

/// An interface implemented by all the possible messages shared by the application over TCP Sockets.
public sealed interface Message extends Serializable permits BrokerJoinRequest, BrokerJoinResponse, BrokerRemoval, ClientIdAssignment, ClientIdRequest, EntryCommit, EntryPropagation, EntryPropagationAck, Heartbeat, HeartbeatAck, NewLeaderAnnouncement, NewLeaderNomination, NewLeaderNominationAck, ReadRequest, ReadResponse, RequestCompletedAck, WriteRequest, WriteResponse
{
}
