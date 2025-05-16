package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.NewLeaderAnnouncement;
import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/// A class containing all election-related information for the broker.
public class ElectionInfo {
    private final AtomicBoolean electionInProgress = new AtomicBoolean(false);
    private final AtomicInteger bestCandidate = new AtomicInteger(-1);
    private final AtomicInteger bestCandidateLogLength = new AtomicInteger(0);
    private final Set<Integer> receivedAcks = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<Integer, Integer> ioFailureCounts = new ConcurrentHashMap<>();
    private final List<Runnable> postElectionCallbacks = new CopyOnWriteArrayList<>();

    private final ScheduledExecutorService nominationAckTimeoutExecutor = Executors.newScheduledThreadPool(10);
    private final ConcurrentMap<Integer, ScheduledFuture<?>> nominationAckTimeoutFutures = new ConcurrentHashMap<>();
    private static final int NOMINATION_ACK_TIMEOUT_MS = 3000;

    /// Resets the election-related information as they were before starting the new leader election.
    public void stopElection() {
        electionInProgress.set(false);
        bestCandidate.set(-1);
        bestCandidateLogLength.set(0);
        receivedAcks.clear();
        ioFailureCounts.clear();
        cancelNominationAckTimeouts();
    }

    /// Updates the stored best-candidate with the given one.
    public void updateBestCandidate(int id, int logLength) {
        bestCandidate.set(id);
        bestCandidateLogLength.set(logLength);
        receivedAcks.clear();
        cancelNominationAckTimeouts();
    }
    /// Increment and return the number of I/O failures for this broker during the current election
    public int incrementIoFailures(int brokerId) {
        return ioFailureCounts.merge(brokerId, 1, Integer::sum);
    }


    /// Adds the given brokerId to the set of brokers who sent an ACK for the current broker nomination.
    public void addReceivedAck(int id) {
        receivedAcks.add(id);
        nominationAckTimeoutFutures.remove(id).cancel(false);
    }

    /// Returns whether there is an election currently in progress or not.
    public boolean isElectionInProgress() {
        return electionInProgress.get();
    }

    /// Returns true if there wasn't an election currently in progress (and false otherwise),
    /// and if there wasn't starts it by atomically changing the
    /// `electionInProgress` flag.
    ///
    /// **NOTE:** it doesn't update all the other flags, so the `updateBestCandidate`
    /// method should still be called after!
    public boolean wasntElectionInProgress() {
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

    /// Adds a new callback to the list of functions executed after an election is completed.
    public void addPostElectionCallback(Runnable callback) {
        postElectionCallbacks.add(callback);
    }

    /// Executes all the post-election callbacks set.
    public void executePostElectionCallbacks() {
        for (Runnable callback : postElectionCallbacks) {
            callback.run();
        }
    }

    public void startNominationAckTimeouts(Set<Integer> peerIds, int myId, SharedState sharedState, NetworkManager networkManager, HeartbeatManager heartbeatManager) {
        // start timeouts for ACKs from peers
        for (Integer peerId : peerIds) {
            if (peerId != myId) {
                ScheduledFuture<?> future = nominationAckTimeoutExecutor.schedule(() -> onNominationAckTimeoutExpired(
                        myId,
                        peerId,
                        sharedState,
                        networkManager,
                        heartbeatManager
                ), NOMINATION_ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS);
                nominationAckTimeoutFutures.put(peerId, future);
            }
        }
    }

    private void cancelNominationAckTimeouts() {
        for (Future<?> timeout : nominationAckTimeoutFutures.values()) {
            timeout.cancel(false);
            nominationAckTimeoutFutures.remove(timeout.hashCode());
        }
    }

    /// Callback for nomination ack timeout expiration.
    private void onNominationAckTimeoutExpired(int myId, int peerId, SharedState sharedState, NetworkManager networkManager, HeartbeatManager heartbeatManager) {
        // remove Ack timeout
        nominationAckTimeoutFutures.remove(peerId).cancel(false);

        if (bestCandidate.get() == myId) {
            System.out.println("[INFO]: No nomination Ack has been received from broker " + peerId + " before timeout expired. Will considering it as failed.");

            // remove failed broker
            sharedState.removeBrokerAddress(peerId);

            // re-check received ACKs
            int activeBrokersCount = sharedState.getBrokersCount();
            int receivedAcksCount = receivedAcks.size();
            System.out.println("[INFO]: Current ACK count: " + receivedAcksCount + "/" + activeBrokersCount);

            // if all ACKs have been received become new leader
            if (receivedAcksCount >= activeBrokersCount - 1) {
                System.out.println("[INFO]: Received ACKs from all " + activeBrokersCount + " active brokers. Becoming new leader");

                // change leader
                sharedState.setNewLeaderId(myId);

                // terminate the election phase
                stopElection();
                executePostElectionCallbacks();

                // restart the heartbeat manager
                heartbeatManager.restart();

                // broadcast new leader announcement
                List<LogEntry> completeLog = sharedState.getCompleteLog();
                networkManager.broadcastMessage(new NewLeaderAnnouncement(myId, completeLog), myId, sharedState);
            }
        }
    }

}
