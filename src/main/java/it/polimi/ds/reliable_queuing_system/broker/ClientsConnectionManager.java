package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.utils.Constants;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/// A runnable that will handle the connection-with-clients side of the broker
public class ClientsConnectionManager implements Runnable {
    private final ExecutorService clientsManagerExecutor = Executors.newFixedThreadPool(Constants.maxClientsPerBroker);
    private int nextClientIdAvailable = 0;  //TODO: this will be part of the shared state controlled by the leader

    @Override
    public void run() {
        try (ServerSocket clientsEndpoint = new ServerSocket(Constants.clientEndpointPort)) {
            System.out.println("Broker ready to accept clients connection on port " + Constants.clientEndpointPort + "...");

            // Whenever a new client connects to the server, assign its handling to a dedicated thread
            while (!clientsEndpoint.isClosed()) {
                Socket socket = clientsEndpoint.accept();

                clientsManagerExecutor.submit(new ClientHandler(socket, nextClientIdAvailable));

                nextClientIdAvailable++;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
