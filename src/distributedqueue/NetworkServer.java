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
 * A simple TCP server that handles requests from clients or other brokers
 * in a concurrent manner (each connection is handled in its own thread).
 *
 * For production, you should manage concurrency more robustly (e.g., thread pools),
 * but this example demonstrates a minimal approach.
 */
public class NetworkServer {

    private static volatile boolean running = false;
    private static ServerSocket serverSocket;
    private static Thread serverThread;

    /**
     * Launches the server in a new thread that listens on the specified port.
     * All incoming connections are handled.
     */
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
                    // Each connection is processed in its own thread
                    handleClient(broker, clientSocket);
                } catch (IOException e) {
                    if (!running) {
                        // Server was stopped intentionally
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

    /**
     * Stops the server listening on the given port.
     */
    public static synchronized void stopServer(int port) {
        running = false;
        if (serverSocket != null) {
            try {
                serverSocket.close();
            } catch (IOException e) {
                // Ignore any error on close
            }
        }
        serverSocket = null;
        System.out.println("NetworkServer stopped on port " + port);
    }

    /**
     * Spawns a new thread to handle the client connection.
     */
    private static void handleClient(BrokerNode broker, Socket clientSocket) {
        new Thread(() -> {
            try (Socket socket = clientSocket;
                 BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
                 PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

                String request = in.readLine();
                if (request != null) {
                    String response = processRequest(broker, request);
                    out.println(response);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }).start();
    }

    /**
     * Processes the request based on a simple text protocol.
     * Supported commands:
     *   REPLICATE|<queueName>|<data>
     *   REGISTER|<followerHost>|<followerPort>
     *   CREATE|<queueName>
     *   APPEND|<queueName>|<data>
     *   READ|<queueName>|<clientId>
     *   SNAPSHOT
     *
     * Returns an appropriate response string.
     */
    private static String processRequest(BrokerNode broker, String request) {
        String[] parts = request.split(NetworkUtils.MSG_SEPARATOR);
        if (parts.length == 0) {
            return "ERROR|Invalid request";
        }

        String command = parts[0].toUpperCase();

        switch (command) {
            case "REPLICATE": {
                // REPLICATE|<queueName>|<data>
                if (parts.length < 3) {
                    return "ERROR|Missing replicate arguments";
                }
                String queueName = parts[1];
                int data;
                try {
                    data = Integer.parseInt(parts[2]);
                } catch (NumberFormatException e) {
                    return "ERROR|Invalid data integer";
                }
                // Follower's replicateDataToFollowers is effectively a local store operation
                broker.replicateDataToFollowers(queueName, data);
                return "OK";
            }

            case "REGISTER": {
                // REGISTER|<followerHost>|<followerPort>
                // This is only valid if the broker is a leader and can register a follower
                if (!broker.isLeader()) {
                    return "ERROR|Not leader";
                }
                if (parts.length < 3) {
                    return "ERROR|Missing register arguments";
                }
                if (!(broker instanceof LeaderBroker leaderBroker)) {
                    return "ERROR|Unknown leader type";
                }
                String followerHost = parts[1];
                int followerPort;
                try {
                    followerPort = Integer.parseInt(parts[2]);
                } catch (NumberFormatException e) {
                    return "ERROR|Invalid followerPort integer";
                }
                leaderBroker.registerFollower(followerHost, followerPort);
                return "OK";
            }

            case "CREATE": {
                // CREATE|<queueName>
                if (parts.length < 2) {
                    return "ERROR|Missing queue name";
                }
                String queueName = parts[1];
                broker.createQueue(queueName);
                return "OK";
            }

            case "APPEND": {
                // APPEND|<queueName>|<data>
                if (parts.length < 3) {
                    return "ERROR|Missing arguments for append";
                }
                String queueName = parts[1];
                int data;
                try {
                    data = Integer.parseInt(parts[2]);
                } catch (NumberFormatException e) {
                    return "ERROR|Invalid data integer";
                }
                broker.appendData(queueName, data);
                return "OK";
            }

            case "READ": {
                // READ|<queueName>|<clientId>
                if (parts.length < 3) {
                    return "ERROR|Missing arguments for read";
                }
                String queueName = parts[1];
                String clientId = parts[2];
                Integer value = broker.readData(queueName, clientId);
                return (value == null) ? "null" : value.toString();
            }

            case "SNAPSHOT": {
                // Return a simple serialization of all queue data
                Map<String, List<Integer>> allData = broker.getAllQueueData();
                // Example format: queue1=1,2,3;queue2=5,10
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
                return "ERROR|Unknown command:" + command;
        }
    }
}