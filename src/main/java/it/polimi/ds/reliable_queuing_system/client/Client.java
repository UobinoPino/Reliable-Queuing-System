package it.polimi.ds.reliable_queuing_system.client;

import it.polimi.ds.reliable_queuing_system.messages.*;
import it.polimi.ds.reliable_queuing_system.utils.Constants;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.List;
import java.util.Scanner;

/// An executable class representing a client.
public class Client {
    private static final Scanner scanner = new Scanner(System.in);
    private static String brokerIp;
    private static int brokerPort;
    private static Integer clientId;
    private static Integer nextOperationId = 0;

    public static void main(String[] args) {
        System.out.println("======== RELIABLE QUEUING SYSTEM: CLIENT ========");

        connectToBroker();

        String input;
        do {
            System.out.println("\nWhat do you want to do?");
            System.out.println("\tr) Read the new values from a queue");
            System.out.println("\tw) Write a new value in a queue");
            System.out.println("\tq) Quit the application");
            input = scanner.nextLine();

            switch (input.toLowerCase()) {
                case "r" -> performRead();
                case "w" -> performWrite();
                case "q" -> { }
                default -> System.out.println("[ERROR]: Invalid command. Please enter either 'r', 'w' or 'q'");
            }
        } while (!input.equalsIgnoreCase("q"));

        System.out.println("Goodbye");
        scanner.close();
    }

    /// Requests the user to input the address of a known broker, and obtains a client id from it.
    private static void connectToBroker() {
        while (brokerIp == null) {
            System.out.print("Please enter the address (<ip>:<port>) of a known broker: ");
            String brokerAddress = scanner.nextLine();

            requestClientId(brokerAddress);
        }
    }

    /// Tries to send to the broker at `brokerIp:brokerPort` a [ClientIdRequest], in order to obtain a
    /// new client id that will be stored in the `clientId` field.
    private static void requestClientId(String brokerAddress) {
        // open a new socket with the known broker
        try {
            brokerIp = brokerAddress.split(":")[0];
            brokerPort = Integer.parseInt(brokerAddress.split(":")[1]);
        } catch (ArrayIndexOutOfBoundsException | NumberFormatException e) {
            brokerIp = null;
            System.out.println("[ERROR]: Invalid broker address format. Please specify an address as <ip>:<port>.");
            return;
        }

        try (Socket socket = new Socket(brokerIp, brokerPort)) {
            ObjectOutputStream toBroker = new ObjectOutputStream(socket.getOutputStream());
            toBroker.flush();
            ObjectInputStream fromBroker = new ObjectInputStream(socket.getInputStream());

            // request a new ID to the broker
            toBroker.writeObject(new ClientIdRequest());
            toBroker.flush();

            // wait for the broker to reply with an id
            while (clientId == null) {
                try {
                    Message res = (Message) fromBroker.readObject();
                    if (res instanceof ClientIdAssignment assignment) {
                        clientId = assignment.clientId();
                    }
                } catch (ClassNotFoundException | ClassCastException ignored) {
                    System.out.println("[WARN]: Unknown message received, it will be ignored.");
                }
            }
        } catch (IOException e) {
            brokerIp = null;
            System.out.println("[ERROR]: Unable to reach the broker at the given IP, please specify another one.");
        }
    }

    /// Requests the user to input the id of the queue that should be read,
    /// then tries to send to the broker at `brokerIp:brokerPort` a [ReadRequest] for it
    /// and prints the results.
    private static void performRead() {
        // collect the id of the queue to read
        System.out.print("Please insert the id of the queue you want to read from: ");
        String queueId = scanner.nextLine();

        // open a socket with the known broker
        try (Socket socket = new Socket(brokerIp, brokerPort)) {
            ObjectOutputStream toBroker = new ObjectOutputStream(socket.getOutputStream());
            toBroker.flush();
            ObjectInputStream fromBroker = new ObjectInputStream(socket.getInputStream());

            // send a read request to the broker
            toBroker.writeObject(new ReadRequest(queueId, clientId, nextOperationId++));
            toBroker.flush();

            // wait for the broker to reply
            socket.setSoTimeout(Constants.maxWaitForBrokerResponse);
            List<Integer> queueValues = null;
            while (queueValues == null) {
                try {
                    Message res = (Message) fromBroker.readObject();
                    if (res instanceof ReadResponse readResponse) {
                        queueValues = readResponse.values();
                    }
                } catch (ClassNotFoundException | ClassCastException ignored) {
                    System.out.println("[WARN]: Unknown message received, it will be ignored.");
                }
            }

            // print the new values of the read queue
            if (queueValues.isEmpty()) {
                System.out.println("No new values has been added to queue '" + queueId + "' since last reading.");
            } else {
                System.out.println("New values in queue '" + queueId + "' has been found: " + queueValues);
            }
        } catch (IOException e) {
            brokerIp = null;
            System.out.println("[ERROR]: Unable to reach the broker at the given IP, please specify another one.");
            connectToBroker();  //FIXME: calling this method like that would give the client a new id and so future reads will not take its offsets into account.
        }
    }

    private static void performWrite() {
        //TODO: implement
    }
}
