package it.polimi.ds.reliable_queuing_system.utils;

import java.io.Serializable;

/// A record that represents the address of a node, with its IP and Port.
public record Address(
        String ip,
        Integer port
) implements Serializable {
}
