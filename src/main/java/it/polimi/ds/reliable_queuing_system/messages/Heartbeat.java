package it.polimi.ds.reliable_queuing_system.messages;

/// Message sent periodically from the leader to the followers to check that they are still available.
public record Heartbeat() implements Message {
}
