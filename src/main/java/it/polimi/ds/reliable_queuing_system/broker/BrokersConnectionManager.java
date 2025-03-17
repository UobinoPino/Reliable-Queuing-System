//package it.polimi.ds.reliable_queuing_system.broker;
//import it.polimi.ds.reliable_queuing_system.utils.Constants;
//import java.io.IOException;
//import java.net.ServerSocket;
//import java.net.Socket;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//
//
//public class BrokersConnectionManager implements Runnable {
//    public BrokersConnectionManager(int myId, int brokersFacingPort, SharedState sharedState) {
//        this.myId = myId;
//        this.brokersFacingPort = brokersFacingPort;
//        this.sharedState = sharedState;
//    }
//
//    private final int myId;
//    private final int brokersFacingPort;
//    private final SharedState sharedState;
//    private final ExecutorService brokersManagerExecutor = Executors.newFixedThreadPool(Constants.maxBrokers);
//
//    @Override
//    public void run() {
//        // Start the heartbeat thread
//        HeartbeatManager heartbeatManager = new HeartbeatManager(myId, sharedState);
//        Thread hbThread = new Thread(heartbeatManager, "HeartbeatManager-" + myId);
//        hbThread.start();
//        try (ServerSocket brokerEndpoint = new ServerSocket(brokersFacingPort)) {
//            System.out.println("Broker ready to accept other brokers' connections on port " + brokersFacingPort + "...");
//
//            System.out.println("ip address: " + brokerEndpoint.getInetAddress().getHostAddress());
//
//            // Accept connections from other brokers
//            while (!brokerEndpoint.isClosed()) {
//                Socket socket = brokerEndpoint.accept();
//
//                brokersManagerExecutor.submit(new BrokerHandler(socket, myId, sharedState));
//            }
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//}
