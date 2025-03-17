//package it.polimi.ds.reliable_queuing_system.broker;
//
//import it.polimi.ds.reliable_queuing_system.messages.BrokerJoinRequest;
//import it.polimi.ds.reliable_queuing_system.messages.BrokerJoinResponse;
//
//import java.io.ObjectInputStream;
//import java.io.ObjectOutputStream;
//import java.net.Socket;
//import java.util.Arrays;
//import java.util.InputMismatchException;
//import java.util.Scanner;
//
//public class OldBroker {
//    public static void main(String[] args) {
//        // obtain the ip of the machine this broker is run on
//        final String ip = getBrokerIp();
//
//        // obtain the ports this broker will use from the user
//        final int brokersFacingPort = getBrokerPort("Please enter the port this broker should use to communicate with other brokers: ");
//        final int clientsFacingPort = getBrokerPort("Please enter the port this broker should use to communicate with clients: ");
//
//        // obtain the correct sharedState and brokerId, depending on if this broker is the first broker of the system or not
//        final SharedState sharedState;
//        final int myId;
//        if (Arrays.asList(args).contains("--first")) {
//            sharedState = new SharedState();
//            myId = sharedState.getNewBrokerId();
//            sharedState.addBrokerAddress(myId, ip + ":" + brokersFacingPort);
//            sharedState.setNewLeaderId(myId);
//        } else {
//            Scanner scanner = new Scanner(System.in);
//            System.out.print("Please enter the address (<ip>:<port>) of a known broker: ");
//            String existingBrokerAddress = scanner.nextLine();
//
//            String existingBrokerIp;
//            int existingBrokerPort;
//            try {
//                String[] addressParts = existingBrokerAddress.split(":");
//                if (addressParts.length != 2) {
//                    throw new IllegalArgumentException("Address must be in the format <ip>:<port>");
//                }
//                existingBrokerIp = addressParts[0];
//                existingBrokerPort = Integer.parseInt(addressParts[1]);
//            } catch (Exception e) {
//                System.out.println("Invalid broker address: " + e.getMessage());
//                System.exit(1);
//                return; // This helps the compiler understand the control flow
//            }
//
//            try (Socket socket = new Socket(existingBrokerIp, existingBrokerPort)) {
//                ObjectOutputStream toExistingBroker = new ObjectOutputStream(socket.getOutputStream());
//                toExistingBroker.flush();
//                ObjectInputStream fromExistingBroker = new ObjectInputStream(socket.getInputStream());
//
//                // Send join request
//                toExistingBroker.writeObject(new BrokerJoinRequest(ip, brokersFacingPort));
//                toExistingBroker.flush();
//
//                // Wait for the response with the shared state
//                Object response = fromExistingBroker.readObject();
//                if (!(response instanceof BrokerJoinResponse)) {
//                    System.out.println("Unexpected response from broker. Expected BrokerJoinResponse");
//                    System.exit(1);
//                    return;
//                }
//
//                BrokerJoinResponse joinResponse = (BrokerJoinResponse) response;
//                myId = joinResponse.newBrokerId();
//                sharedState = joinResponse.sharedState();
//
//                // Register this broker's address
//                //TODO: not needed? (the leader already added the new broker to the list before returning the sharedState)
////                sharedState.addBrokerAddress(myId, ip + ":" + brokersFacingPort);
//
//            } catch (Exception e) {
//                System.out.println("Failed to connect to existing broker: " + e.getMessage());
//                e.printStackTrace();
//                System.exit(1);
//                return;
//            }
//
//        }
//
//        // start thread that will manage connections with other brokers
//        Thread brokersConnectionThread = new Thread(new BrokersConnectionManager(myId, brokersFacingPort, sharedState));
//        brokersConnectionThread.start();
//
//        // start thread that will manage connections with the clients
//        Thread clientsConnectionThread = new Thread(new ClientsConnectionManager(myId, clientsFacingPort, sharedState));
//        clientsConnectionThread.start();
//    }
//
//    /// Returns the ip of the node this class is executed on
//    /// (or localhost ip if unable to determine it).
//    private static String getBrokerIp() {
//        try {
//            return java.net.InetAddress.getLocalHost().getHostAddress();
//        } catch (java.net.UnknownHostException e) {
//            return "127.0.0.1"; // Default to localhost if unable to determine
//        }
//    }
//
//    /// Requests the user to input a valid port by using the given prompt
//    /// and returns it.
//    private static int getBrokerPort(String prompt) {
//        final Scanner scanner = new Scanner(System.in);
//        int chosenPort = -1;
//        while (chosenPort < 49152 || chosenPort > 65535) {
//            System.out.print(prompt);
//            try {
//                chosenPort = scanner.nextInt();
//            } catch (InputMismatchException e) {
//                scanner.nextLine();
//                chosenPort = -1;
//            }
//            if (chosenPort < 49152 || chosenPort > 65535) {
//                System.out.println("Invalid port. Please insert a number between 49152 and 65535.");
//            }
//        }
//        return chosenPort;
//    }
//}