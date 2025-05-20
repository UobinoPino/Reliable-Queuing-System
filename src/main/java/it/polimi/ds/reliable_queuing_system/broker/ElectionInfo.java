package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.NewLeaderAnnouncement;
import it.polimi.ds.reliable_queuing_system.messages.NewLeaderNomination;
import it.polimi.ds.reliable_queuing_system.utils.LogEntry;

import java.util.List;
import java.util.Map;
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

    private final Map<Integer, CompletableFuture<Void>> nominationAckWaitingFutures = new ConcurrentHashMap<>();
    private final Map<Integer, Integer> failedNominationAckTimeouts = new ConcurrentHashMap<>();
    private static final int NOMINATION_ACK_TIMEOUT_MS = 30000;

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
        if (nominationAckWaitingFutures.containsKey(id)) {
            System.out.println("[DEBUG]: Received ACK from broker " + id + ". Marking its future as completed.");
            nominationAckWaitingFutures.get(id).complete(null);
        } else {
            System.out.println("[DEBUG]: Received ACK from broker " + id + " even if I was not waiting for it.");
        }
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
        System.out.println("[DEBUG]: Starting nomination ACK timeouts for peers: " + peerIds);

        for (Integer peerId : peerIds) {
            if (peerId != myId) {
                CompletableFuture<Void> future = new CompletableFuture<>();
                nominationAckWaitingFutures.put(peerId, future);

                future
                        .orTimeout(NOMINATION_ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        .exceptionally(ex -> {
                            if (ex instanceof TimeoutException) {
                                onNominationAckTimeoutExpired(
                                        myId,
                                        peerId,
                                        sharedState,
                                        networkManager,
                                        heartbeatManager
                                );
                            }
                            return null;
                        });
            }
        }
    }

    private void cancelNominationAckTimeouts() {
        System.out.println("[DEBUG]: Cancelling nomination ACK timeouts for peers: " + nominationAckWaitingFutures.keySet());

        // cancel every scheduled timeout
        for (CompletableFuture<Void> future : nominationAckWaitingFutures.values()) {
            future.complete(null);
        }

        // remove all entries at once
        nominationAckWaitingFutures.clear();
    }

    /// Callback for nomination ack timeout expiration.
    private void onNominationAckTimeoutExpired(int myId, int peerId, SharedState sharedState, NetworkManager networkManager, HeartbeatManager heartbeatManager) {
        System.out.println("[DEBUG]: Nomination Ack timeout expired for broker " + peerId);

        int failedTimeouts = failedNominationAckTimeouts.getOrDefault(peerId, 0);

        boolean considerFailed = false;
        if (failedTimeouts < 3) {
            System.out.println("[DEBUG]: Broker " + peerId + " failed to send nomination ACK. Will retry for the " + (failedTimeouts + 1) + " time.");

            failedNominationAckTimeouts.put(peerId, failedTimeouts + 1);
            boolean success = networkManager.sendMessage(new NewLeaderNomination(myId, sharedState.getLogLength()), sharedState.getBrokerAddress(peerId));

            if (success) {
                CompletableFuture<Void> future = new CompletableFuture<>();
                nominationAckWaitingFutures.put(peerId, future);
                future
                        .orTimeout(NOMINATION_ACK_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        .exceptionally(ex -> {
                            if (ex instanceof TimeoutException) {
                                onNominationAckTimeoutExpired(
                                        myId,
                                        peerId,
                                        sharedState,
                                        networkManager,
                                        heartbeatManager
                                );
                            }
                            return null;
                        });
            }
            else {
                System.out.println("[DEBUG]: Failed to re-send nomination message to broker " + peerId + ". Will consider it as failed.");
                considerFailed = true;
            }
        } else {
            System.out.println("[DEBUG]: Broker " + peerId + " failed to send nomination ACK for the 3rd time. Will consider it as failed.");
            considerFailed = true;
        }

        if (considerFailed) {
            CompletableFuture<Void> nominationAckWaitingFuture = nominationAckWaitingFutures.get(peerId);
            if (nominationAckWaitingFuture != null) {
                if (bestCandidate.get() == myId) {
                    System.out.println("[INFO]: No nomination Ack has been received from broker " + peerId + " before timeout expired. Will considering it as failed.");

                    // remove failed broker
                    sharedState.removeBrokerAddress(peerId);

                    // re-check received ACKs
                    int activeBrokersCount = sharedState.getBrokersCount();
                    int receivedAcksCount = receivedAcks.size();
                    System.out.println("[INFO]: Current ACK count: " + receivedAcksCount + "/" + (activeBrokersCount-1));

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
            else {
                System.out.println("[ERROR]: No nomination Ack timeout was scheduled for broker " + peerId);
            }
        }
    }

}
