package it.polimi.ds.reliable_queuing_system.client;

/// Enumeration of the possible states for the client.
public enum ClientState {
    READY,
    WAITING_ID,
    WAITING_READ,
    WAITING_WRITE,
}
