package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.Heartbeat;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/*
  A simple heartbeat manager that:
  1) If this broker is the leader, sends heartbeats to all followers periodically.
  2) Checks for missed heartbeats/acks at both leader and followers to detect failures.
*/
public class HeartbeatManager implements Runnable {

    private final int myId;
    private final SharedState sharedState;
    // For demonstration, we send heartbeats and check them every 3 seconds
    private static final long HEARTBEAT_INTERVAL_MS = 3000;
    // Consider brokers dead if no ack or heartbeat was received within 9 seconds
    private static final long HEARTBEAT_TIMEOUT_MS = 9000;

    public HeartbeatManager(int myId, SharedState sharedState) {
        this.myId = myId;
        this.sharedState = sharedState;
    }

    @Override
    public void run() {
        while (true) {
            try {
                if (sharedState.isLeader(myId)) {
                    // Leader -> send heartbeat to all other brokers
                    int totalBrokersAssigned = sharedState.getBrokersCount();
                    for (int brokerId = 0; brokerId < totalBrokersAssigned; brokerId++) {
                        if (brokerId == myId) {
                            continue;
                        }
                        String address = sharedState.getBrokerAddress(brokerId);
                        if (address != null) {
                            sendHeartbeatTo(brokerId);
                        }
                    }
                    // Check if followers have timed out
                    sharedState.checkFollowerTimeouts(HEARTBEAT_TIMEOUT_MS);
                } else {
                    // Follower -> check if the leader is still sending heartbeats
                    sharedState.checkLeaderTimeout(myId, HEARTBEAT_TIMEOUT_MS);
                }
                Thread.sleep(HEARTBEAT_INTERVAL_MS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
    }

    private void sendHeartbeatTo(int brokerId) {
        String address = sharedState.getBrokerAddress(brokerId);
        if (address == null) return;
        String[] addressParts = address.split(":");
        String ip = addressParts[0];
        int port = Integer.parseInt(addressParts[1]);

        try (Socket sock = new Socket(ip, port)) {
            ObjectOutputStream out = new ObjectOutputStream(sock.getOutputStream());
            out.flush();
            out.writeObject(new Heartbeat());
            out.flush();
        } catch (Exception e) {
            // If we fail here, the broker might be down
            System.out.println("Failed to send heartbeat to broker " + brokerId + ": " + e.getMessage());
        }
    }
}