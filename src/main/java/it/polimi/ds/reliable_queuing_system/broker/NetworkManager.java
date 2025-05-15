package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.Message;
import it.polimi.ds.reliable_queuing_system.utils.Address;
import it.polimi.ds.reliable_queuing_system.utils.PooledConnection;

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
    public NetworkManager(ElectionInfo electionInfo) {
        this.electionInfo = electionInfo;
    }

    public static final int SOCKET_TIMEOUT = 5000;
    private static final int PER_PEER_POOL_SIZE = 5000;

    // pool of live connections (socket + cached ObjectOutputStream)
    private final ConcurrentMap<Address, BlockingQueue<PooledConnection>> pools = new ConcurrentHashMap<>();

    public final ElectionInfo electionInfo;

    private PooledConnection borrowConn(Address peer) throws IOException {
        // Get or create a connection queue for this peer
        BlockingQueue<PooledConnection> q = pools.computeIfAbsent(peer,
                __ -> new LinkedBlockingQueue<>(PER_PEER_POOL_SIZE));

        // Try to get an existing connection from the pool
        PooledConnection pc = q.poll();
        if (pc != null && pc.socket().isConnected() && !pc.socket().isClosed()) {
            return pc;
        }

        // Otherwise create a new connection
        Socket sock = new Socket();
        SocketAddress sa = new InetSocketAddress(peer.ip(), peer.port());
        sock.connect(sa, SOCKET_TIMEOUT);
        ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
        oos.flush();

        return new PooledConnection(sock, oos);
    }

    private void returnConn(Address peer, PooledConnection pc) {
        if (pc == null || pc.socket().isClosed()) {
            return;
        }

        BlockingQueue<PooledConnection> q = pools.get(peer);
        if (q == null || !q.offer(pc)) {
            // pool full or missing -> close
            try {
                pc.socket().close();
            } catch (IOException ignored) {}
        }
    }

    public void sendMessage(Message msg, Address peer) {
        PooledConnection pc = null;
        try {
            pc = borrowConn(peer);
            // reuse the same ObjectOutputStream
            pc.outStream().writeObject(msg);
            pc.outStream().reset();    // clear stream cache so that the object is sent as a new (even if sent previously over the same stream)
            pc.outStream().flush();
            System.out.println("[INFO]: Sent message " + msg + " to " + peer + "...");
            returnConn(peer, pc);
        } catch (IOException e) {
            // on fail drop this conn
            if (pc != null) {
                try {
                    pc.socket().close();
                } catch (IOException ignored) {}
            }
            System.out.println("Failed to send message to " + peer + ":  it has probably crashed.");
        }
    }

    public void forwardMessageToLeader(Message msg, SharedState state) {
        if (electionInfo.isElectionInProgress()) {
            System.out.println("[INFO]: Election in progress, message forwarding to leader skipped");
            return;
        }
        int leaderId = state.getLeaderId();
        Address leader = state.getBrokerAddress(leaderId);

        PooledConnection pc = null;
        try {
            pc = borrowConn(leader);
            // reuse the same ObjectOutputStream
            pc.outStream().writeObject(msg);
            pc.outStream().reset();    // clear stream cache so that the object is sent as a new (even if sent previously over the same stream)
            pc.outStream().flush();
            System.out.println("[INFO]: Message " + msg + " forwarded to leader " + leaderId);
            returnConn(leader, pc);
        } catch (IOException e) {
            // on fail drop this conn
            if (pc != null) {
                try {
                    pc.socket().close();
                } catch (IOException ignored) {}
            }

            // and throw an exception to the caller
            System.out.println("[WARN]: Unable to forward the message " + msg + " to the leader " + leaderId + ". It has probably crashed.");
            throw new RuntimeException(e);
        }
    }

    public void broadcastMessage(Message msg, int myId, SharedState state) {
        Map<Integer, Address> brokerAddresses = state.getBrokerAddresses();
        Set<Integer> brokerIds = brokerAddresses.keySet();

        System.out.println("[INFO]: Trying to broadcast message " + msg + " to brokers " + brokerIds + "...");

        for (Integer id : brokerIds) {
            if (id != myId) {
                Address addr = brokerAddresses.get(id);
                if (addr != null) {  // Safety check to ensure we have the address
                    PooledConnection pc;
                    try {
                        pc = borrowConn(addr);
                        // reuse the same ObjectOutputStream
                        pc.outStream().writeObject(msg);
                        pc.outStream().reset();    // clear stream cache so that the object is sent as a new (even if sent previously over the same stream)
                        pc.outStream().flush();
                        returnConn(addr, pc);
                    } catch (IOException e) {
                        System.out.println("[ERROR]: Unable to broadcast the message to node " + id + ". It has probably crashed");
                    }

                }
            }
        }
    }

    /** Graceful shutdown: close all pooled sockets */
    public void shutdown() {
        pools.forEach((peer, q) -> {
            PooledConnection pc;
            while ((pc = q.poll()) != null) {
                try { pc.socket().close(); } catch (IOException ignored) {}
            }
        });
        pools.clear();
    }

    public void cleanupConnectionsFor(Address peer) {
        BlockingQueue<PooledConnection> q = pools.get(peer);
        if (q != null) {
            // Close all existing connections for this peer
            PooledConnection pc;
            while ((pc = q.poll()) != null) {
                try { pc.socket().close(); } catch (IOException ignored) {}
            }
            // Remove the entire queue for this peer
            pools.remove(peer);
            System.out.println("[INFO]: Cleaned up connection pool for " + peer);
        }
    }

}