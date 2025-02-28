package it.polimi.ds.reliable_queuing_system.messages;

/// ACK message sent by the followers as a reply to [BrokerAddition].
public record BrokerAdditionAck(
        int brokerId
) implements Message {
}
