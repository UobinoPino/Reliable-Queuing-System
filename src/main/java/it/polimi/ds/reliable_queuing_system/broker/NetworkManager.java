package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.Message;
import it.polimi.ds.reliable_queuing_system.utils.Address;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;

/**
 * NetworkManager with proper pooling of Socket + ObjectOutputStream so that
 * we only ever write one stream header per connection.
 */
public class NetworkManager {
    public static final int socketTimeout = 5000;
    private static final int PER_PEER_POOL_SIZE = 5000;

    // pool of live connections (socket + cached ObjectOutputStream)
    private static final ConcurrentMap<Address, BlockingQueue<PooledConn>> pools = new ConcurrentHashMap<>();

    public static final ElectionInfo electionInfo = Broker.electionInfo;


    private static class PooledConn {
        final Socket socket;
        final ObjectOutputStream out;
        PooledConn(Socket s, ObjectOutputStream o) { socket = s; out = o; }
    }

    private static PooledConn borrowConn(Address peer) throws IOException {
        // Get or create a connection queue for this peer
        BlockingQueue<PooledConn> q = pools.computeIfAbsent(peer,
                __ -> new LinkedBlockingQueue<>(PER_PEER_POOL_SIZE));

        // Try to get an existing connection from the pool
        PooledConn pc = q.poll();
        if (pc != null && pc.socket.isConnected() && !pc.socket.isClosed()) {
            return pc;
        }
        // Otherwise create a new connection
        Socket sock = new Socket();
        SocketAddress sa = new InetSocketAddress(peer.ip(), peer.port());
        sock.connect(sa, socketTimeout);
        ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
        oos.flush();
        return new PooledConn(sock, oos);
    }

    private static void returnConn(Address peer, PooledConn pc) {
        if (pc == null || pc.socket.isClosed()) return;
        BlockingQueue<PooledConn> q = pools.get(peer);
        if (q == null || !q.offer(pc)) {
            // pool full or missing -> close
            try { pc.socket.close(); } catch (IOException ignored) {}
        }
    }

    public static void sendMessage(Message msg, Address peer) {
        PooledConn pc = null;
        try {
            pc = borrowConn(peer);
            // reuse the same ObjectOutputStream
            pc.out.writeObject(msg);
            pc.out.reset();    // clear handle cache so next object is not deduped
            pc.out.flush();
            System.out.println("[INFO]: Sent message " + msg + " to " + peer + "...");
            returnConn(peer, pc);
        } catch (IOException e) {
            // on fail drop this conn
            if (pc != null) try { pc.socket.close(); } catch (IOException ignored) {}
            System.out.println("Failed to send message to " + peer + ":  it has probably crashed.");
        }
    }

    public static void forwardMessageToLeader(Message msg, SharedState state) {
        if (electionInfo.isElectionInProgress()) {
            System.out.println("[INFO]: Election in progress, message forwarding to leader skipped");
            return;
        }
        int leaderId = state.getLeaderId();
        Address leader = state.getBrokerAddress(leaderId);

        PooledConn pc = null;
        try {
            pc = borrowConn(leader);
            // reuse the same ObjectOutputStream
            pc.out.writeObject(msg);
            pc.out.reset();    // clear handle cache so next object is not deduped
            pc.out.flush();
            System.out.println("[INFO]: Message " + msg + " forwarded to leader " + leaderId);
            returnConn(leader, pc);
        } catch (IOException e) {

            // on fail drop this conn
           System.out.println("[WARN]: Unable to forward the message " + msg + " to the leader " + leaderId + ". It has probably crashed.");

            throw new RuntimeException(e);
        }
    }

    public static void broadcastMessage(Message msg, int myId, SharedState state) {
        Map<Integer, Address> brokerAddresses = state.getBrokerAddresses();
        Set<Integer> brokerIds = brokerAddresses.keySet();

        System.out.println("[INFO]: Trying to broadcast message " + msg + " to brokers " + brokerIds + "...");

        for (Integer id : brokerIds) {
            if (id != myId) {
                Address addr = brokerAddresses.get(id);
                if (addr != null) {  // Safety check to ensure we have the address
                    //sendMessage(msg, addr);
                    PooledConn pc = null;
                    try {
                        pc = borrowConn(addr);
                        // reuse the same ObjectOutputStream
                        pc.out.writeObject(msg);
                        pc.out.reset();    // clear handle cache so next object is not deduped
                        pc.out.flush();
                        returnConn(addr, pc);
                    } catch (IOException e) {
                        System.out.println("[ERROR]: Unable to broadcast the message to node " + id + ". It has probably crashed");

                        // If we're in an election and encounter a failure, remove the broker
//                        if (electionInfo.isElectionInProgress()) {
//                            state.removeBrokerAddress(id);
//                            System.out.println("[INFO]: REMOVED crashed broker " + id + " from broker list");
//                        }
                    }

                }
            }
        }
    }

    /** Graceful shutdown: close all pooled sockets */
    public static void shutdown() {
        pools.forEach((peer, q) -> {
            PooledConn pc;
            while ((pc = q.poll()) != null) {
                try { pc.socket.close(); } catch (IOException ignored) {}
            }
        });
        pools.clear();
    }
    public static void cleanupConnectionsFor(Address peer) {
        BlockingQueue<PooledConn> q = pools.get(peer);
        if (q != null) {
            // Close all existing connections for this peer
            PooledConn pc;
            while ((pc = q.poll()) != null) {
                try { pc.socket.close(); } catch (IOException ignored) {}
            }
            // Remove the entire queue for this peer
            pools.remove(peer);
            System.out.println("[INFO]: Cleaned up connection pool for " + peer);
        }
    }

}