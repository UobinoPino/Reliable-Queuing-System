package it.polimi.ds.reliable_queuing_system.broker;

public class Broker {
    public static void main(String[] args) {
        //TODO: connect to the other brokers and retrieve the current shared state...
        SharedState sharedState = new SharedState();  // (temporary)

        // start thread that will manage connections with other brokers
        Thread brokersConnectionThread = new Thread(new BrokersConnectionManager(sharedState));
        brokersConnectionThread.start();

        //start thread that will manage connections with the clients
        Thread clientsConnectionThread = new Thread(new ClientsConnectionManager(sharedState));
        clientsConnectionThread.start();
    }
}
