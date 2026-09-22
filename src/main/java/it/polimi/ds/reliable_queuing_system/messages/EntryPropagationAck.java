package it.polimi.ds.reliable_queuing_system.messages;

import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

/// ACK message sent by followers in reply to [EntryPropagation].
///
/// It carries the id of the broker that sent it, so that the leader can count
/// *distinct* ACKs (a duplicate or retransmitted ACK coming from the same follower
/// must not be counted twice) and decide when the entry reached a majority quorum.
public record EntryPropagationAck(
        LogEntry logEntry,
        int brokerId
) implements Message {
}