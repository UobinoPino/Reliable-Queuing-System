package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.Message;
import it.polimi.ds.reliable_queuing_system.utils.Address;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.Map;

/// A class containing useful static methods for inter-broker communication.
public class NetworkManager {
    /// Forwards the given message to the current system leader
    public static void forwardMessageToLeader(Message msg, int myId, SharedState sharedState) {
        int leaderId = sharedState.getLeaderId();

        if (leaderId != myId) {
            Address leaderAddr = sharedState.getBrokerAddress(leaderId);

            try(Socket socket = new Socket(leaderAddr.ip(), leaderAddr.port())) {
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(msg);
                out.flush();
            } catch (IOException e) {
                throw new RuntimeException(e);
                //TODO: replace with proper error handling
                // (maybe it should trigger a new election?)
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

        for (Integer id : brokerAddresses.keySet()) {
            if (id != myId) {
                Address addr = brokerAddresses.get(id);
                try(Socket socket = new Socket(addr.ip(), addr.port())) {
                    ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                    out.writeObject(message);
                    out.flush();
                } catch (IOException e) {
                    System.out.println("[ERROR]: Unable to broadcast the message to node " + id + ". It has probably crashed");
//                    throw new RuntimeException(e);
                    //TODO: replace with proper error handling
                    // (maybe it should trigger node removal?)
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
