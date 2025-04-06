package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.Message;
import it.polimi.ds.reliable_queuing_system.utils.Address;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.Set;

import static it.polimi.ds.reliable_queuing_system.broker.Broker.electionInfo;

/// A class containing useful static methods for inter-broker communication.
public class NetworkManager {
    /// Forwards the given message to the current system leader
    public static void forwardMessageToLeader(Message msg, int myId, SharedState sharedState) {

        if (electionInfo.isElectionInProgress()) {
            System.out.println("[INFO]: Election in progress, message forwarding to leader skipped");
            return;
        }

        int leaderId = sharedState.getLeaderId();

        if (leaderId != myId) {
            Address leaderAddr = sharedState.getBrokerAddress(leaderId);

            try(Socket socket = new Socket(leaderAddr.ip(), leaderAddr.port())) {
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(msg);
                out.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);

            }
        }
        else {
            System.out.println("[ERROR]: Tried to forward a message to the current leader while being the current leader.");
            //TODO: how should we handle it?
        }


    }

    /// Broadcasts the given [Message] to all other brokers in the system
    public static void broadcastMessage(Message message, int myId, SharedState sharedState) {
        Map<Integer, Address> brokerAddresses = sharedState.getBrokerAddresses();
        Set<Integer> brokerIds = brokerAddresses.keySet();

        System.out.println("[INFO]: Broadcasting to brokers: " + brokerIds);

        for (Integer id : brokerIds) {
            if (id != myId) {
                Address addr = brokerAddresses.get(id);
                if (addr != null) {  // Safety check to ensure we have the address
                    try(Socket socket = new Socket(addr.ip(), addr.port())) {
                        ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                        out.writeObject(message);
                        out.flush();
                    } catch (IOException e) {
                        System.out.println("[ERROR]: Unable to broadcast the message to node " + id + ". It has probably crashed");

                        // If we're in an election and encounter a failure, remove the broker
                        if (electionInfo.isElectionInProgress()) {
                            sharedState.removeBrokerAddress(id);
                            System.out.println("[INFO]: Removed crashed broker " + id + " from broker list");
                        }
                    }
                }
            }
        }
    }

    /// Send the given [Message] to the given [Address]
    public static void sendMessage(Message message, Address address) {
        try(Socket socket = new Socket(address.ip(), address.port())) {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(message);
            out.flush();
        } catch (IOException e) {
            System.out.println("Failed to send message to " + address + ":  it has probably crashed.");
            //TODO: should we do something here?
        }
    }
}
