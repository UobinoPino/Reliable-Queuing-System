package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.*;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;

public class BrokerHandler implements Runnable{

    public BrokerHandler(Socket socket, int myId, SharedState sharedState) throws IOException {
        this.myId = myId;
        this.sharedState = sharedState;
        this.toBroker = new ObjectOutputStream(socket.getOutputStream());
        toBroker.flush();
        this.fromBroker = new ObjectInputStream(socket.getInputStream());
    }
    private final int myId;
    private final SharedState sharedState;
    private final ObjectInputStream fromBroker;
    private final ObjectOutputStream toBroker;
    public void run() {
        while (true) {
            try {
                Message msg = (Message) fromBroker.readObject();

                if (msg == null) {
                    break;
                }
                //mega switch
                switch (msg) {
                    /*
                    case BrokerAddition brokerAddition -> handleBrokerAddition(brokerAddition);
                    case BrokerAdditionAck brokerAdditionAck -> handleBrokerAdditionAck(brokerAdditionAck);
                    case BrokerJoinRequest joinRequest -> handleBrokerJoinRequest(joinRequest);
                    case BrokerJoinResponse joinResponse -> handleBrokerJoinResponse(joinResponse);
                    case BrokerRemoval brokerRemoval -> handleBrokerRemoval(brokerRemoval);
                    case ClientIdAssignment assignment -> handleClientIdAssignment(assignment);
                    case ClientIdRequest clientIdRequest -> forwardToLeader(clientIdRequest);
                    case ClientOffsetsUpdate offsetsUpdate -> handleClientOffsetsUpdate(offsetsUpdate);
                    case EntryCommit commit -> handleEntryCommit(commit);
                    case EntryPropagation entryPropagation -> handleEntryPropagation(entryPropagation);
                    case EntryPropagationAck ack -> handleEntryPropagationAck(ack);
                    case ReadRequest readRequest -> forwardToLeader(readRequest);
                    case Heartbeat heartbeat -> handleHeartbeat(heartbeat);
                    case HeartbeatAck heartbeatAck -> handleHeartbeatAck(heartbeatAck);
                    case NewLeaderAnnouncement announcement -> handleNewLeaderAnnouncement(announcement);
                    case NewLeaderNomination nomination -> handleNewLeaderNomination(nomination);
                    case NewLeaderNominationAck nominationAck -> handleNewLeaderNominationAck(nominationAck);

                    case ReadResponse readResponse -> handleReadResponse(readResponse);
                    case ReadConfirmation readConfirmation -> handleReadConfirmation(readConfirmation);
                    case WriteRequest writeRequest -> forwardToLeader(writeRequest);
                    case WriteResponse writeResponse -> handleWriteResponse(writeResponse);
                    */
                    default -> System.out.println("Unknown message type: Leonardo abbiamo aggiunto altri messaggi?" + msg.getClass().getSimpleName());

                }
            }
            catch (ClassNotFoundException | ClassCastException ignored) {
                // Ignore unknown message types
            }
            catch (EOFException e) {
                // Connection closed
                break;
            }
            catch (IOException e) {
                e.printStackTrace();
                break;
            }
        }
    }



}

