package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.*;
import it.polimi.ds.reliable_queuing_system.utils.Address;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.*;

public class Broker {
    private static final Scanner scanner = new Scanner(System.in);

    private static Address brokerAddress;

    private static SharedState sharedState;
    private static Integer brokerId;
    private static HeartbeatManager heartbeatManager;
    private static LogManager logManager;
    private static MessageDispatcher messageDispatcher;

    private static BrokerState brokerState;

    public static final ElectionInfo electionInfo = new ElectionInfo();

    public static void main(String[] args) {
        System.out.println("======== RELIABLE QUEUING SYSTEM: BROKER ========");

        // ask user to choose port
        String brokerIp = obtainBrokerIp();
        Integer brokerPort = obtainBrokerPort();
        brokerAddress = new Address(brokerIp, brokerPort);

        // obtain the correct sharedState and brokerId, depending on if this broker is the first broker of the system or not
        if (Arrays.asList(args).contains("--first")) {
            sharedState = new SharedState();
            brokerId = sharedState.getNewBrokerId();
            sharedState.addBrokerAddress(brokerId, brokerAddress);
            System.out.println("[INFO]: First, Broker " + brokerId + " started with address: " + brokerAddress);
            sharedState.setNewLeaderId(brokerId);

            initializeManagers();
        }
        else {
            // ask the user to specify the address of a known broker
            Address knownBrokerAddress = obtainKnownBrokerAddress();

            // send the join request to the known broker
            try(Socket socket = new Socket(knownBrokerAddress.ip(), knownBrokerAddress.port())) {
                ObjectOutputStream toExistingBroker = new ObjectOutputStream(socket.getOutputStream());
                toExistingBroker.writeObject(new BrokerJoinRequest(brokerAddress));
                toExistingBroker.flush();

                brokerState = BrokerState.WAITING_JOIN;
            } catch (IOException e) {
                System.out.println("[FATAL ERROR]: " + e.getMessage());
                System.exit(1);
                //TODO: maybe we could re-ask another address instead of crashing
            }
        }

        try(ServerSocket serverSocket = new ServerSocket(brokerAddress.port())) {
            System.out.println("[INFO]: Broker ready to receive messages at port: " + brokerAddress.port());

            while (!serverSocket.isClosed()) {
                Socket socket = serverSocket.accept();
                ObjectInputStream in  = new ObjectInputStream(socket.getInputStream());

                try {
                    Message message = (Message) in.readObject();

                    if (brokerState == BrokerState.WAITING_JOIN) {
                        if (message instanceof BrokerJoinResponse brokerJoinResponse) {
                            brokerId = brokerJoinResponse.newBrokerId();
                            System.out.println("[INFO]: Not first, Broker " + brokerId + " started with address: " + brokerAddress);
                            sharedState = brokerJoinResponse.sharedState();
                            brokerState = BrokerState.READY;

                            initializeManagers();
                        }
                        else {
                            System.out.println("[INFO]: Received message before joining the cluster. It will be ignored.");
                        }
                    }
                    else {
                        messageDispatcher.dispatch(message);
                    }
                } catch (ClassNotFoundException | ClassCastException ignored) {
                    System.out.println("[INFO]: Unknown message received, it will be ignored.");
                }
            }
        } catch(IOException e) {
            System.out.println("[FATAL ERROR] " + e.getMessage());
            System.exit(1);
        }
    }


    /// Returns the ip of the node this class is executed on
    /// (or localhost ip if unable to determine it).
    private static String obtainBrokerIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "127.0.0.1"; // Default to localhost if unable to determine
        }
    }

    /// Requests the user to input a valid port and returns it.
    private static Integer obtainBrokerPort() {
        Integer port = null;
        while (port == null) {
            System.out.print("Enter the port the client should listen to: ");
            try {
                int input = scanner.nextInt();
                scanner.nextLine();
                if (input < 1024 || input > 65535) {
                    System.out.println("[ERROR]: Invalid port number. Please choose a number between 1024 and 65535.");
                } else {
                    port = input;
                }
            } catch (InputMismatchException e) {
                System.out.println("[ERROR]: Invalid port number. Please choose a number between 1024 and 65535.");
            }
        }

        return port;
    }

    /// Requests the user to input a valid address for a known broker in the format \<ip\>:\<port\>
    /// and returns it.
    private static Address obtainKnownBrokerAddress() {
        String[] addr = null;
        while (addr == null) {
            System.out.print("Enter the address of a known broker (<ip>:<port>): ");
            String input = scanner.nextLine();
            String[] parts = input.split(":");
            if (parts.length != 2) {
                System.out.println("[ERROR]: Invalid address. Please specify a valid IP address with the format <ip>:<port>.");
            }
            else {
                try {
                    int port = Integer.parseInt(parts[1]);
                    if (port < 1024 || port > 65535) {
                        System.out.println("[ERROR]: Invalid port number. Please choose a number between 1024 and 65535.");
                    }
                    else {
                        addr = parts;
                    }
                } catch (NumberFormatException e) {
                    System.out.println("[ERROR]: Invalid port number. Please choose a number between 1024 and 65535.");
                }
            }
        }

        return new Address(addr[0], Integer.parseInt(addr[1]));
    }

    /// Create an instance for each one of the Manager classes used by the Broker.
    private static void initializeManagers() {
        heartbeatManager = new HeartbeatManager(brokerId, sharedState, electionInfo);
        logManager = new LogManager(brokerId, sharedState);
        messageDispatcher = new MessageDispatcher(
                brokerId,
                sharedState,
                heartbeatManager,
                logManager,
                electionInfo
        );
    }
}