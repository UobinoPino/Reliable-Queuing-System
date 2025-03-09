package it.polimi.ds.reliable_queuing_system.broker;

import java.util.Arrays;
import java.util.InputMismatchException;
import java.util.Scanner;

public class Broker {
    public static void main(String[] args) {
        // obtain the ip of the machine this broker is run on
        final String ip = getBrokerIp();

        // obtain the ports this broker will use from the user
        final int brokersFacingPort = getBrokerPort("Please enter the port this broker should use to communicate with other brokers: ");
        final int clientsFacingPort = getBrokerPort("Please enter the port this broker should use to communicate with clients: ");

        // obtain the correct sharedState and brokerId, depending on if this broker is the first broker of the system or not
        final SharedState sharedState;
        final int myId;
        if (Arrays.asList(args).contains("--first")) {
            sharedState = new SharedState();
            myId = sharedState.getNewBrokerId();
            sharedState.addBrokerAddress(myId, ip + ":" + brokersFacingPort);
            sharedState.setNewLeaderId(myId);
        } else {
            //TODO: connect to the other brokers and retrieve the current shared state...
            System.out.println("non-first brokers are still a WIP");
            return;
        }

        // start thread that will manage connections with other brokers
        Thread brokersConnectionThread = new Thread(new BrokersConnectionManager(myId, brokersFacingPort, sharedState));
        brokersConnectionThread.start();

        //start thread that will manage connections with the clients
        Thread clientsConnectionThread = new Thread(new ClientsConnectionManager(myId, clientsFacingPort, sharedState));
        clientsConnectionThread.start();
    }

    /// Returns the ip of the node this class is executed on
    /// (or localhost ip if unable to determine it).
    private static String getBrokerIp() {
        try {
            return java.net.InetAddress.getLocalHost().getHostAddress();
        } catch (java.net.UnknownHostException e) {
            return "127.0.0.1"; // Default to localhost if unable to determine
        }
    }

    /// Requests the user to input a valid port by using the given prompt
    /// and returns it.
    private static int getBrokerPort(String prompt) {
        final Scanner scanner = new Scanner(System.in);

        int chosenPort = -1;
        // TODO: check if lower ports are also ok
        while (chosenPort < 49152 || chosenPort > 65535) {
            System.out.print(prompt);

            try {
                chosenPort = scanner.nextInt();
            } catch (InputMismatchException e) {
                scanner.nextLine();
                chosenPort = -1;
            }

            if (chosenPort < 49152 || chosenPort > 65535) {
                System.out.println("Invalid port. Please insert a number between 49152 and 65535.");
            }
        }

        return chosenPort;
    }
}
