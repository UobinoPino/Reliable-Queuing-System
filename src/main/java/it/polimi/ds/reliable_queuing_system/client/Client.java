package it.polimi.ds.reliable_queuing_system.client;

import it.polimi.ds.reliable_queuing_system.utils.MsgType;
import it.polimi.ds.reliable_queuing_system.utils.Constants;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.net.UnknownHostException;
import java.util.List;
import java.util.Scanner;

public class Client {
    private static final Scanner scanner = new Scanner(System.in);
    private static String brokerIp;
    private static Integer clientId;

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
                default -> System.out.println("Invalid command. Please enter either 'r', 'w' or 'q'");
            }
        } while (!input.equalsIgnoreCase("q"));

        System.out.println("Goodbye");
        scanner.close();
    }

    public static void connectToBroker() {
        while (brokerIp == null) {
            System.out.print("Please enter the IP address of a known broker: ");
            brokerIp = scanner.nextLine();

            requestClientId();
        }
    }

    public static void requestClientId() {
        // open a new socket with the known broker
        try (Socket socket = new Socket(brokerIp, Constants.clientEndpointPort)) {
            BufferedReader fromBroker = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter toBroker = new PrintWriter(socket.getOutputStream(), true);

            // request a new ID to the broker
            JSONObject req = new JSONObject();
            req.put("type", MsgType.request_client_id);
            toBroker.println(req);

            // wait for the broker to reply with an id
            while (clientId == null) {
                String res = fromBroker.readLine();
                JSONObject resObj = new JSONObject(res);
                MsgType resType = MsgType.valueOf(resObj.getString("type"));
                if (resType == MsgType.assign_client_id) {
                    clientId = resObj.getInt("id");
                }
            }
        } catch (UnknownHostException e) {
            brokerIp = null;
            System.out.println("Unable to reach the broker at the given IP, please specify another one.");
        } catch (IOException e) {
            e.printStackTrace();  //TODO: handle better?
        }
    }

    private static void performRead() {
        // collect the id of the queue to read
        System.out.print("Please insert the id of the queue you want to read from: ");
        String queueId = scanner.nextLine();

        // open a socket with the known broker
        try (Socket socket = new Socket(brokerIp, Constants.clientEndpointPort)) {
            BufferedReader fromBroker = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter toBroker = new PrintWriter(socket.getOutputStream(), true);

            // send a read request to the broker
            JSONObject msg = new JSONObject();
            msg.put("type", MsgType.read_req);
            msg.put("queue_id", queueId);
            msg.put("client_id", clientId);
            toBroker.println(msg);

            // wait for the broker to reply
            socket.setSoTimeout(Constants.maxWaitForBrokerResponse);
            List<Integer> queueValues = null;
            while (queueValues == null) {
                String res = fromBroker.readLine();
                JSONObject resObj = new JSONObject(res);
                MsgType resType = MsgType.valueOf(resObj.getString("type"));
                if(resType == MsgType.read_res) {
                    queueValues = resObj.getJSONArray("values").toList().stream().map(el -> (Integer) el).toList();
                }
            }

            // print the new values of the read queue
            if (queueValues.isEmpty()) {
                System.out.println("No new values has been added to queue '" + queueId + "' since last reading.");
            } else {
                System.out.println("New values in queue '" + queueId + "' has been found: " + queueValues);
            }
        } catch (UnknownHostException | SocketTimeoutException e) {
            brokerIp = null;
            System.out.println("Unable to reach the broker at the given IP, please specify another one.");
            connectToBroker();
        } catch (IOException e) {
            e.printStackTrace();  //TODO: handle better?
        }
    }

    private static void performWrite() {
        //TODO: implement
    }
}
