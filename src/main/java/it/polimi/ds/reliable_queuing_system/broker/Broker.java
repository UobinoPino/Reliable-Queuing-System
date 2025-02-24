package it.polimi.ds.reliable_queuing_system.broker;

public class Broker {
    public static void main(String[] args) {
        //TODO: connect to the other brokers and retrieve info about leader, log, queues,...

        // start thread that will manage connections with other brokers
        Thread brokersConnectionThread = new Thread(new BrokersConnectionManager());
        brokersConnectionThread.start();

        //start thread that will manage connections with the clients
        Thread clientsConnectionThread = new Thread(new ClientsConnectionManager());
        clientsConnectionThread.start();
    }
}
