package it.polimi.ds.reliable_queuing_system.test;

public record LoadSimResults(
        int clientId,
        int totalOperations,
        int successfulOperations,
        int failedOperations,
        long totalTime,
        double throughput
) {
}
