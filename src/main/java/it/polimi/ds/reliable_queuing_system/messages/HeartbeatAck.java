package it.polimi.ds.reliable_queuing_system.messages;

/// ACK message sent by the followers as a reply to [Heartbeat].
public record HeartbeatAck(Integer brokerId) implements Message {
}
