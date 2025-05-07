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
    public static final int socketTimeout = 3000;
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
        BlockingQueue<PooledConn> q = pools.computeIfAbsent(peer,
                __ -> new LinkedBlockingQueue<>(PER_PEER_POOL_SIZE));
        PooledConn pc = q.poll();
        if (pc != null && pc.socket.isConnected() && !pc.socket.isClosed()) {
            return pc;
        }
        // create new socket+OOS header
        Socket sock = new Socket();
        SocketAddress sa = new InetSocketAddress(peer.ip(), peer.port());
        sock.connect(sa, socketTimeout);
        ObjectOutputStream oos = new ObjectOutputStream(sock.getOutputStream());
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
            System.out.println("[INFO]: Sent " + msg + " → " + peer);
            returnConn(peer, pc);
        } catch (IOException e) {
            // on fail drop this conn
            if (pc != null) try { pc.socket.close(); } catch (IOException ignored) {}
            System.out.println("[WARN]: sendMessage failed to " + peer + ": " + e.getMessage());
        }
    }

    public static void forwardMessageToLeader(Message msg, SharedState state) {
        if (electionInfo.isElectionInProgress()) return;
        Address leader = state.getBrokerAddress(state.getLeaderId());
        sendMessage(msg, leader);
    }

    public static void broadcastMessage(Message msg, int myId, SharedState state) {
        Set<Map.Entry<Integer,Address>> peers = state.getBrokerAddresses().entrySet();
        for (var e : peers) {
            if (e.getKey() == myId) continue;
            sendMessage(msg, e.getValue());
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

}