package it.polimi.ds.reliable_queuing_system.broker;

public class BrokersConnectionManager implements Runnable {
    public BrokersConnectionManager(SharedState sharedState) {
        this.sharedState = sharedState;
    }

    private final SharedState sharedState;

    @Override
    public void run() {
        //TODO: implement
        System.out.println("TODO :)");
    }
}
