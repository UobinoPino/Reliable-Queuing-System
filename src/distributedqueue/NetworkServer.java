package distributedqueue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;

/**
 * A simple TCP server that handles requests from clients or brokers in a concurrent manner.
 * Each connection is handled in its own thread.
 *
 * Updated to support "FORWARDED_APPEND" requests from followers:
 *   FORWARDED_APPEND|<queueName>|<data> -> The leader calls broker.appendData(...) and
 *   then replicates the new data to followers.
 */
public class NetworkServer {

    private static volatile boolean running = false;
    private static ServerSocket serverSocket;
    private static Thread serverThread;

    public static synchronized void startServer(BrokerNode broker, int port) {
        if (running) {
            return;
        }
        running = true;
        try {
            serverSocket = new ServerSocket(port);
        } catch (IOException e) {
            e.printStackTrace();
            running = false;
            return;
        }

        serverThread = new Thread(() -> {
            while (running) {
                try {
                    Socket clientSocket = serverSocket.accept();
                    handleClient(broker, clientSocket);
                } catch (IOException e) {
                    if (!running) {
                        break;
                    }
                    e.printStackTrace();
                }
            }
        });
        serverThread.setDaemon(true);
        serverThread.start();

        System.out.println("NetworkServer started on port " + port);
    }

    public static synchronized void stopServer(int port) {
        running = false;
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                // ignore
            }
        }
        serverSocket = null;
        System.out.println("NetworkServer stopped on port " + port);
    }

    private static void handleClient(BrokerNode broker, Socket clientSocket) {
        new Thread(() -> {
            try (Socket socket = clientSocket;
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                String request = in.readLine();
                if (request != null) {
                   // String response = processRequest(broker, request);
                    String response = processRequest(broker, request.split("\\|"));
                    if (response == null) {
                        response = "null";
                    }
                    out.println(response);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    /**
     * Minimal text-based protocol:
     *   FORWARDED_READ|<queueName>|<clientId>
     *   FORWARDED_APPEND|<queueName>|<data>
     *   REPLICATE|<queueName>|<data>
     *   UPDATEOFFSET|<queueName>|<clientId>|<offset>
     *   REGISTER|<followerHost>|<followerPort>
     *   CREATE|<queueName>
     *   APPEND|<queueName>|<data>
     *   READ|<queueName>|<clientId>
     *   SNAPSHOT
     */
    private static String processRequest(BrokerNode broker, String[] parts) {
       // String[] parts = request.split(NetworkUtils.MSG_SEPARATOR);
        if (parts.length == 0) {
            return "ERROR|Invalid request";
        }

        String command = parts[0].toUpperCase();

        switch (command) {
            case "FORWARDED_READ": {
                if (parts.length < 3) {
                    return "null";
                }
                String qName = parts[1];
                String cId = parts[2];
                Integer val = broker.readData(qName, cId);
                return val == null ? "null" : val.toString();
            }
            case "FORWARDED_APPEND": {
                // Only a leader can handle an append and replicate. If this is a follower, it won't do anything special.
                // But typically, we expect this to arrive at the leader, which calls appendData in the standard way.
                if (!broker.isLeader()) {
                    return "ERROR|Not leader";
                }
                if (parts.length < 3) {
                    return "ERROR|Missing args for FORWARDED_APPEND";
                }
                String qName = parts[1];
                int data;
                try {
                    data = Integer.parseInt(parts[2]);
                } catch (NumberFormatException e) {
                    return "ERROR|Invalid data integer";
                }
                // The broker here should be a LeaderBroker instance
                broker.appendData(qName, data);
                return "OK";
            }
            case "REPLICATE": {
                if (parts.length < 3) {
                    return "ERROR|Missing replicate arguments";
                }
                String qName = parts[1];
                int data;
                try {
                    data = Integer.parseInt(parts[2]);
                } catch (NumberFormatException e) {
                    return "ERROR|Invalid data integer";
                }
                broker.replicateDataToFollowers(qName, data);
                return "OK";
            }
            case "UPDATEOFFSET": {
                if (parts.length < 4) {
                    return "ERROR|Missing offset arguments";
                }
                String qName = parts[1];
                String cId = parts[2];
                int offset;
                try {
                    offset = Integer.parseInt(parts[3]);
                } catch (NumberFormatException e) {
                    return "ERROR|Invalid offset integer";
                }
                broker.updateClientOffset(qName, cId, offset);
                return "OK";
            }
            case "REGISTER": {
                if (!broker.isLeader()) {
                    return "ERROR|Not leader";
                }
                if (parts.length < 3) {
                    return "ERROR|Missing register arguments";
                }
                String followerHost = parts[1];
                int followerPort;
                try {
                    followerPort = Integer.parseInt(parts[2]);
                } catch (NumberFormatException e) {
                    return "ERROR|Invalid followerPort integer";
                }
                if (broker instanceof LeaderBroker leaderBroker) {
                    leaderBroker.registerFollower(followerHost, followerPort);
                }
                return "OK";
            }
            case "CREATE": {
                if (parts.length < 2) {
                    return "ERROR|Missing queueName";
                }
                String qName = parts[1];
                broker.createQueue(qName);
                return "OK";
            }
            case "APPEND": {
                if (parts.length < 3) {
                    return "ERROR|Missing arguments for append";
                }
                String qName = parts[1];
                int data;
                try {
                    data = Integer.parseInt(parts[2]);
                } catch (NumberFormatException e) {
                    return "ERROR|Invalid data integer";
                }
                broker.appendData(qName, data);
                return "OK";
            }
            case "READ": {
                if (parts.length < 3) {
                    return "ERROR|Missing arguments for read";
                }
                String qName = parts[1];
                String cId = parts[2];
                Integer val = broker.readData(qName, cId);
                return val == null ? "null" : val.toString();
            }
            case "SNAPSHOT": {
                Map<String, List<Integer>> allData = broker.getAllQueueData();
                StringBuilder sb = new StringBuilder();
                boolean firstQueue = true;
                for (Map.Entry<String, List<Integer>> entry : allData.entrySet()) {
                    if (!firstQueue) {
                        sb.append(";");
                    } else {
                        firstQueue = false;
                    }
                    sb.append(entry.getKey()).append("=");
                    List<Integer> values = entry.getValue();
                    for (int i = 0; i < values.size(); i++) {
                        sb.append(values.get(i));
                        if (i < values.size() - 1) {
                            sb.append(",");
                        }
                    }
                }
                return sb.toString();
            }
            default:
                return "ERROR|Unknown command: " + command;
        }
    }
}