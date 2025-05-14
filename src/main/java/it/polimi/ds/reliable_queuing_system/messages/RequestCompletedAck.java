package it.polimi.ds.reliable_queuing_system.messages;

import it.polimi.ds.reliable_queuing_system.utils.Address;

public record RequestCompletedAck(
        Address clientAddress,
        int operationId
) implements Message {
}
