package it.polimi.ds.reliable_queuing_system.broker;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/// A class containing all election-related information for the broker.
public class ElectionInfo {
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private final AtomicInteger bestCandidate = new AtomicInteger(-1);
    private final AtomicInteger bestCandidateLogLength = new AtomicInteger(0);
    private final Set<Integer> receivedAcks = ConcurrentHashMap.newKeySet();

    /// Resets the election-related information as they were before starting the new leader election.
    public void stopElection() {
        electionInProgress.set(false);
        bestCandidate.set(-1);
        bestCandidateLogLength.set(0);
        receivedAcks.clear();
    }

    /// Updates the stored best-candidate with the given one.
    public void updateBestCandidate(int id, int logLength) {
        bestCandidate.set(id);
        bestCandidateLogLength.set(logLength);
        receivedAcks.clear();
    }

    /// Adds the given brokerId to the set of brokers who sent an ACK for the current broker nomination.
    public void addReceivedAck(int id) {
        receivedAcks.add(id);
    }

    /// Returns whether there is an election currently in progress or not.
    public boolean isElectionInProgress() {
        return electionInProgress.get();
    }

    /// Returns whether there was an election currently in progress or not,
    /// and if there wasn't starts it by atomically changing the
    /// `electionInProgress` flag.
    ///
    /// **NOTE:** it doesn't update all the other flags, so the `startElection`
    /// method should still be called after!
    public boolean wasElectionInProgress() {
        return electionInProgress.compareAndSet(false, true);
    }

    /// Returns the id of the current best-candidate.
    public int getBestCandidate() {
        return bestCandidate.get();
    }

    /// Returns the log length of the current best-candidate.
    public int getBestCandidateLogLength() {
        return bestCandidateLogLength.get();
    }

    /// Returns the number of brokers that already sent an ACK for the current broker nomination.
    public int getReceivedAcksCount() {
        return receivedAcks.size();
    }
}
