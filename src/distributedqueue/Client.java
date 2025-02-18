package distributedqueue;

import java.util.UUID;

/**
 * Represents a client in the system. A client:
 *  - Has a unique ID
 *  - Connects to a broker (leader/follower)
 *  - Issues createQueue, appendData, readData
 *  - Can reconnect to another broker if needed, continuing from last offset
 *
 * Requirements addressed:
 * 5 (connect to leader for writes), 7 (the brokers track the offsets),
 * 8 (multiple clients can exist), 13 (unique client ID), 15 (reconnect to new broker).
 */
public class Client {

    private final String clientId;
    private String connectedBrokerHost;
    private int connectedBrokerPort;

    public Client() {
        this.clientId = UUID.randomUUID().toString();
    }

    public String getClientId() {
        return clientId;
    }

    /**
     * Connect to a broker specified by IP/hostname and port.
     */
    public void connect(String brokerHost, int brokerPort) {
        this.connectedBrokerHost = brokerHost;
        this.connectedBrokerPort = brokerPort;
        System.out.println("Client " + clientId + " connected to broker " + brokerHost + ":" + brokerPort);
    }

    /**
     * Create a new queue on the connected broker (should be the leader).
     */
    public void createQueue(String queueName) {
        if (connectedBrokerHost == null) {
            System.out.println("Client " + clientId + ": No broker connected. Cannot create queue.");
            return;
        }
        NetworkClient.sendCreateQueueRequest(connectedBrokerHost, connectedBrokerPort, queueName);
    }

    /**
     * Append data to an existing queue on the connected broker (should be the leader).
     * @param queueName The queue name.
     * @param data The integer data to append.
     */
    public void appendData(String queueName, int data) {
        if (connectedBrokerHost == null) {
            System.out.println("Client " + clientId + ": No broker connected. Cannot append data.");
            return;
        }
        NetworkClient.sendAppendDataRequest(connectedBrokerHost, connectedBrokerPort, queueName, data);
    }

    /**
     * Read the next data. This can be from a leader or follower.
     * @param queueName The queue name.
     * @return Next integer data or null if none available.
     */
    public Integer readData(String queueName) {
        if (connectedBrokerHost == null) {
            System.out.println("Client " + clientId + ": No broker connected. Cannot read data.");
            return null;
        }
        return NetworkClient.sendReadDataRequest(connectedBrokerHost, connectedBrokerPort, queueName, clientId);
    }

    /**
     * Reconnect to a different broker after a failure.
     */
    public void reconnect(String newBrokerHost, int newBrokerPort) {
        this.connectedBrokerHost = newBrokerHost;
        this.connectedBrokerPort = newBrokerPort;
        System.out.println("Client " + clientId + " reconnected to broker " + newBrokerHost + ":" + newBrokerPort);
    }
}