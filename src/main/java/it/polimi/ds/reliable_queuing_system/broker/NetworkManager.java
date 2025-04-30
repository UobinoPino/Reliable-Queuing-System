package it.polimi.ds.reliable_queuing_system.broker;

import it.polimi.ds.reliable_queuing_system.messages.Message;
import it.polimi.ds.reliable_queuing_system.utils.Address;

import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Set;

import static it.polimi.ds.reliable_queuing_system.broker.Broker.electionInfo;

/// A class containing useful static methods for inter-broker communication.
public class NetworkManager {
    /// Forwards the given message to the current system leader
    public static void forwardMessageToLeader(Message msg, SharedState sharedState) {

        if (electionInfo.isElectionInProgress()) {
            System.out.println("[INFO]: Election in progress, message forwarding to leader skipped");
            return;
        }

        int leaderId = sharedState.getLeaderId();

        Address leaderAddr = sharedState.getBrokerAddress(leaderId);

        try(Socket socket = new Socket(leaderAddr.ip(), leaderAddr.port())) {
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            out.writeObject(msg);
            out.flush();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /// Broadcasts the given [Message] to all other brokers in the system
    public static void broadcastMessage(Message message, int myId, SharedState sharedState) {
        Map<Integer, Address> brokerAddresses = sharedState.getBrokerAddresses();
        Set<Integer> brokerIds = brokerAddresses.keySet();

        for (Integer id : brokerIds) {
            if (id != myId) {
                Address addr = brokerAddresses.get(id);
                if (addr != null) {  // Safety check to ensure we have the address
                    try(Socket socket = new Socket()) {
                        SocketAddress socketAddress = new InetSocketAddress(addr.ip(), addr.port());
                        socket.connect(socketAddress, 1000);  //TODO: check if it works and if so update this 1s timeout with a constant declared somewhere
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

        }
    }
}
