package it.polimi.ds.reliable_queuing_system.broker;

public class BrokersConnectionManager implements Runnable {
    public BrokersConnectionManager(int myId, int brokersFacingPort, SharedState sharedState) {
        this.myId = myId;
        this.brokersFacingPort = brokersFacingPort;
        this.sharedState = sharedState;
    }

    private final int myId;
    private final int brokersFacingPort;
    private final SharedState sharedState;

    @Override
    public void run() {
        //TODO: implement
        System.out.println("Broker connection manager is still a WIP.");
    }
}
