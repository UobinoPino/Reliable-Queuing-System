package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

public class ClientHandler implements Runnable {
    public ClientHandler(Socket socket, SharedState sharedState) throws IOException {
        this.sharedState = sharedState;
        this.toClient = new ObjectOutputStream(socket.getOutputStream());
        toClient.flush();
        this.fromClient = new ObjectInputStream(socket.getInputStream());
    }

    private final SharedState sharedState;
    private final ObjectInputStream fromClient;
    private final ObjectOutputStream toClient;

    @Override
    public void run() {
        while (true) {
            try {
                // wait for incoming messages from the client
                Message msg = (Message) fromClient.readObject();

                // if the connection is closed exit the infinite loop (and terminate the thread)
                if (msg == null) {
                    break;
                }

                // otherwise handle the received message
                switch (msg) {
                    case ClientIdRequest clientIdRequest -> assignClientId(clientIdRequest);
                    case ReadRequest readRequest -> tmpHandleRead(readRequest);
                    default -> System.out.println("Message type not yet implemented");  //TODO: invoke methods to handle each type of message from client instead
                }
            }
            catch (ClassNotFoundException | ClassCastException ignored) {
                // if a message of unknown type is received simply ignore it
            } catch (EOFException e) {
                // if the input stream terminates exit the infinite loop (and terminate the thread)
                break;
            }
            catch (IOException e) {
                e.printStackTrace();  //TODO: handle better?
                break;
            }
        }
    }

    private void assignClientId(ClientIdRequest req) throws IOException {
        toClient.writeObject(new ClientIdAssignment(sharedState.getNewClientId()));
        toClient.flush();
    }

    private void tmpHandleRead(ReadRequest req) throws IOException {
        String queueId = req.queueId();
        List<Integer> values = new ArrayList<>();
        if (queueId.equals("a")) {
            values.add(1);
            values.add(2);
        }

        toClient.writeObject(new ReadResponse(
            req.clientId(),
            req.operationId(),
            values
        ));
        toClient.flush();
    }
}
