package it.polimi.ds.reliable_queuing_system;

/**
 * Demonstrates a client connecting to the distributed queue system to:
 * 1. Create a queue
 * 2. Append data to that queue
 * 3. Read data from that queue
 *
 * You can run this code on any machine (including the leader/follower machines) in the LAN.
 * Make sure to set the correct IP/host and port for the desired broker.
 */
public class ClientMain {

    public static void main(String[] args) {

        // 1. Instantiate a client.
        Client client = new Client();

        // 2. Connect to the LeaderBroker (e.g., "192.168.1.10" and port 5001).
        //    Update to match the actual leader machine's IP/hostname and port in your LAN.
        //client.connect("192.168.1.10", 5001); non da stesso pc
        client.connect("127.0.0.1", 5001);

        // 3. Create a queue on the leader (for demonstration).
        client.createQueue("myQueue");

        // 4. Append some data items to that queue.
        client.appendData("myQueue", 100);
        client.appendData("myQueue", 200);
        client.appendData("myQueue", 300);

        // 5. Read from the leader:
        Integer item = client.readData("myQueue");
        Integer item2 = client.readData("myQueue");
        System.out.println("Client read (leader): " + item);
        System.out.println("Client read (leader): " + item2);

        // 6. (Optional) Reconnect to a FollowerBroker (e.g., 192.168.1.11:5002) to read data:
        //    This is only useful if you have a running FollowerBroker on that machine/port.
        //client.reconnect("192.168.1.11", 5002); non da stesso pc
        client.reconnect("127.0.0.1", 5002);
        client.appendData("myQueue", 400);
        Integer item3 = client.readData("myQueue");
        Integer item4 = client.readData("myQueue");
        System.out.println("Client read (leader): " + item3);
        System.out.println("Client read (leader): " + item4);
        // to fix: when writing from client also the offset needs to be updated? and check also reading

        client.reconnect("127.0.0.1", 5001);
        client.appendData("myQueue", 500);
        Integer item5 = client.readData("myQueue");
        Integer item6 = client.readData("myQueue");

        System.out.println("Client read (follower): " + item5);
        System.out.println("Client read (follower): " + item6);

        // 7. Keep running to allow for additional demonstration commands if desired.
        while (true) {
            try {
                Thread.sleep(2000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }
}