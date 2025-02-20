package it.polimi.ds.reliable_queuing_system;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

/**
 * A utility class for sending various requests to a broker (leader or follower).
 * This updated version reinstates the client-specific request methods used by Client.java:
 *   - sendCreateQueueRequest
 *   - sendAppendDataRequest
 *   - sendReadDataRequest
 *
 * It also includes functions for registering a follower with the leader,
 * and a generic sendRequest for arbitrary commands.
 */
public class NetworkClient {

    /**
     * Registers a follower with a leader.
     * The leader must be listening on leaderHost:leaderPort.
     */
    public static void registerFollower(String leaderHost, int leaderPort, String followerHost, int followerPort) {
        String request = "REGISTER"
                + NetworkUtils.MSG_SEPARATOR + followerHost
                + NetworkUtils.MSG_SEPARATOR + followerPort;
        sendRequest(leaderHost, leaderPort, request);
    }

    /**
     * Sends a CREATE queue request to the broker.
     * Equivalent to: CREATE|<queueName>
     */
    public static void sendCreateQueueRequest(String host, int port, String queueName) {
        String request = "CREATE"
                + NetworkUtils.MSG_SEPARATOR + queueName;
        String response = sendRequest(host, port, request);
        if (response == null || response.startsWith("ERROR")) {
            System.out.println("Create queue failed: " + response);
        }
    }

    /**
     * Sends a request to append data to an existing queue on the broker.
     * Equivalent to: APPEND|<queueName>|<data>
     */
    public static void sendAppendDataRequest(String host, int port, String queueName, int data) {
        String request = "APPEND"
                + NetworkUtils.MSG_SEPARATOR + queueName
                + NetworkUtils.MSG_SEPARATOR + data;
        String response = sendRequest(host, port, request);
        if (response == null || response.startsWith("ERROR")) {
            System.out.println("Append data request failed: " + response);
        }
    }

    /**
     * Sends a request to read the next data item for the specified client ID on the given queue.
     * Equivalent to: READ|<queueName>|<clientId>
     *
     * @param host The broker host.
     * @param port The broker port.
     * @param queueName Name of the queue to read from.
     * @param clientId Client identifier for offset tracking.
     * @return The next data (Integer) or null if none available or error.
     */
    public static Integer sendReadDataRequest(String host, int port, String queueName, String clientId) {
        String request = "READ"
                + NetworkUtils.MSG_SEPARATOR + queueName
                + NetworkUtils.MSG_SEPARATOR + clientId;
        String response = sendRequest(host, port, request);
        if (response == null || response.startsWith("ERROR") || response.equals("null")) {
            return null;
        }
        try {
            return Integer.valueOf(response);
        } catch (NumberFormatException e) {
            return null; // If the broker returned something unexpected
        }
    }

    /**
     * A generic method to send any type of request to the broker and receive a response.
     * Example usage:
     *     sendRequest("127.0.0.1", 5001, "SOME_COMMAND|argA|argB");
     */
    public static String sendRequest(String host, int port, String request) {
        try (Socket socket = new Socket(host, port);
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()))) {

            out.println(request);
            return in.readLine();  // read one line response

        } catch (IOException e) {
            e.printStackTrace();
            return "ERROR|Client connection failed:" + e.getMessage();
        }
    }
}