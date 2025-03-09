package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.utils.Constants;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/// A runnable that will handle the connection-with-clients side of the broker
public class ClientsConnectionManager implements Runnable {
    public ClientsConnectionManager(int myId, int clientsFacingPort, SharedState sharedState) {
        this.myId = myId;
        this.clientsFacingPort = clientsFacingPort;
        this.sharedState = sharedState;
    }

    private final int myId;
    private final int clientsFacingPort;
    private final SharedState sharedState;

    private final ExecutorService clientsManagerExecutor = Executors.newFixedThreadPool(Constants.maxClientsPerBroker);

    @Override
    public void run() {
        try (ServerSocket clientsEndpoint = new ServerSocket(clientsFacingPort)) {
            System.out.println("Broker ready to accept clients connection on port " + clientsFacingPort + "...");

            // Whenever a new client connects to the server, assign its handling to a dedicated thread
            while (!clientsEndpoint.isClosed()) {
                Socket socket = clientsEndpoint.accept();

                clientsManagerExecutor.submit(new ClientHandler(socket, myId, sharedState));
            }
        } catch (IOException e) {
            e.printStackTrace();  //TODO: handle better?
        }
    }
}
