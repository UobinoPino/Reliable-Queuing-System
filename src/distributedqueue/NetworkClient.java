package distributedqueue;

import java.io.*;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A simple synchronous client that sends requests over TCP to a broker's NetworkServer.
 *
 * This class is used by:
 *  - Follower brokers to talk to Leader
 *  - Clients to talk to a Broker
 *  - Leader to replicate to Followers
 *
 * It's a naive example for demonstration only.
 */
public class NetworkClient {

    private static String sendRequest(String host, int port, String request) {
        try (Socket socket = new Socket(host, port)) {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

            out.println(request);
            out.flush();

            return in.readLine();  // read single-line response
        } catch (IOException e) {
            // In real system, handle connection failures, timeouts, etc.
            e.printStackTrace();
            return null;
        }
    }

    public static void sendReplicationRequest(String host, int port, String queueName, int data) {
        String req = "REPLICATE" + NetworkUtils.MSG_JOINER + queueName + NetworkUtils.MSG_JOINER + data;
        sendRequest(host, port, req);
    }

    public static void registerFollower(String leaderHost, int leaderPort, String followerHost, int followerPort) {
        String req = "REGISTER" + NetworkUtils.MSG_JOINER + followerHost + NetworkUtils.MSG_JOINER + followerPort;
        sendRequest(leaderHost, leaderPort, req);
    }

    public static void sendCreateQueueRequest(String host, int port, String queueName) {
        String req = "CREATE" + NetworkUtils.MSG_JOINER + queueName;
        sendRequest(host, port, req);
    }

    public static void sendAppendDataRequest(String host, int port, String queueName, int data) {
        String req = "APPEND" + NetworkUtils.MSG_JOINER + queueName + NetworkUtils.MSG_JOINER + data;
        sendRequest(host, port, req);
    }

    public static Integer sendReadDataRequest(String host, int port, String queueName, String clientId) {
        String req = "READ" + NetworkUtils.MSG_JOINER + queueName + NetworkUtils.MSG_JOINER + clientId;
        String response = sendRequest(host, port, req);
        if (response == null || response.equalsIgnoreCase("null")) {
            return null;
        }
        try {
            return Integer.parseInt(response);
        } catch (NumberFormatException e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public static Map<String, List<Integer>> requestFullDataSnapshot(String host, int port) {
        String req = "SNAPSHOT";
        String resp = sendRequest(host, port, req);
        // Resp is a special serialization: queueName->data1,data2,... (semi-colon separated).
        // This is naive, you might use JSON or another format in production.
        if (resp == null) {
            return null;
        }
        return parseSnapshot(resp);
    }

    private static Map<String, List<Integer>> parseSnapshot(String snapshotStr) {
        // Example format: queueA=1,2,3;queueB=5,10
        // We'll process it into a map
        // This is naive parsing, we skip error checks for brevity
        Map<String, List<Integer>> result = new java.util.HashMap<>();
        if (snapshotStr.trim().isEmpty()) {
            return result;
        }
        String[] queueEntries = snapshotStr.split(";");
        for (String q : queueEntries) {
            String[] parts = q.split("=");
            if (parts.length != 2) continue;
            String queueName = parts[0];
            String dataStr = parts[1];
            if (dataStr.trim().isEmpty()) {
                result.put(queueName, new java.util.ArrayList<>());
                continue;
            }
            String[] dataItems = dataStr.split(",");
            List<Integer> intList = new java.util.ArrayList<>();
            for (String d : dataItems) {
                try {
                    intList.add(Integer.parseInt(d));
                } catch (NumberFormatException e) {
                    e.printStackTrace();
                }
            }
            result.put(queueName, intList);
        }
        return result;
    }
}