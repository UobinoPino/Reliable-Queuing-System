package it.polimi.ds.reliable_queuing_system.test;

import it.polimi.ds.reliable_queuing_system.utils.Address;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class MultiClientLoadSim {
    private static final ExecutorService clientsSimExecutor = Executors.newFixedThreadPool(100);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        System.out.println("======== RELIABLE QUEUING SYSTEM: MULTI-CLIENT LOAD SIMULATION ========");

        Scanner scanner = new Scanner(System.in);

        System.out.print("Enter the known broker address (ip:port) [default: 127.0.0.1:5001]: ");
        String brokerInput = scanner.nextLine().trim();
        Address brokerAddress = brokerInput.isEmpty() ?
                new Address("127.0.0.1", 5001) :
                parseAddress(brokerInput);

        // if loopback address has been specified as broker address, replace it with the actual IP address of the machine
        if (brokerAddress.ip().equals("127.0.0.1")) {
            brokerAddress = new Address(obtainClientIp(), brokerAddress.port());
        }

        System.out.print("Enter the number of clients to simulate concurrently [default: 10]: ");
        String clientsNumInput = scanner.nextLine().trim();
        int clientsNum = clientsNumInput.isEmpty() ?
                10 :
                Integer.parseInt(clientsNumInput);

        System.out.print("Enter the total number of operations each client should perform [default: 100]: ");
        String totalOperationsInput = scanner.nextLine().trim();
        int totalOperations = totalOperationsInput.isEmpty() ?
                100 :
                Integer.parseInt(totalOperationsInput);

        System.out.print("Enter read/write ratio [default: 0.5]: ");
        String readWriteRatioInput = scanner.nextLine().trim();
        double readWriteRatio = readWriteRatioInput.isEmpty() ?
                0.5 :
                Double.parseDouble(readWriteRatioInput);

        System.out.print("Enter queue name prefix [default: queue]: ");
        String queueNamePrefixInput = scanner.nextLine().trim();
        String queueNamePrefix = queueNamePrefixInput.isEmpty() ?
                "queue" :
                queueNamePrefixInput;

        System.out.print("Enter the number of queues each client should use [default: 5]: ");
        String queuesNumInput = scanner.nextLine().trim();
        int queuesNum = queuesNumInput.isEmpty() ?
                5 :
                Integer.parseInt(queuesNumInput);

        List<LoadSimResults> results = new ArrayList<>();
        for (int i = 0; i < clientsNum; i++) {
            int clientNum = i;
            Address brokerAddr = brokerAddress;
            Future<LoadSimResults> simResultsFuture = clientsSimExecutor.submit(() -> startClientSim(clientNum, brokerAddr, totalOperations, readWriteRatio, queueNamePrefix, queuesNum));
            results.add(simResultsFuture.get());
        }

        // aggregate results
        int totOps = results.stream().mapToInt(LoadSimResults::totalOperations).sum();
        int successOps = results.stream().mapToInt(LoadSimResults::successfulOperations).sum();
        int failedOps = results.stream().mapToInt(LoadSimResults::failedOperations).sum();
        long avgTime = results.stream().mapToLong(LoadSimResults::totalTime).sum() / results.size();
        double avgThroughput = results.stream().mapToDouble(LoadSimResults::throughput).sum() / results.size();

        System.out.println("\n====== MULTI-CLIENT SIMULATION COMPLETED ======");
        System.out.println("Average simulation duration: " + avgTime + " ms");
        System.out.println("Total operations issued: " + totOps);
        System.out.println("Total operations completed: " + successOps);
        System.out.println("Total operations failed: " + failedOps);
        System.out.println("Average Throughput: " + avgThroughput + " ops/s");
        System.out.println("================================================");

        clientsSimExecutor.shutdownNow();
    }

    private static LoadSimResults startClientSim(int clientNum, Address brokerAddress, int totalOperations, double readWriteRatio, String queueNamePrefix, int queuesNum) throws InterruptedException {
        String[] queueNames = new String[queuesNum];
        for (int i = 0; i < queuesNum; i++) {
            queueNames[i] = queueNamePrefix + "_" + clientNum + "." + i;
        }

        ClientLoadSim clientSim = new ClientLoadSim(
                new Address(obtainClientIp(), obtainAvailablePort()),
                brokerAddress,
                totalOperations,
                readWriteRatio,
                queueNames
        );

        return clientSim.start();
    }

    private static String obtainClientIp() {
        try {
            return InetAddress.getLocalHost().getHostAddress();
        } catch (UnknownHostException e) {
            return "127.0.0.1";
        }
    }

    private static int obtainAvailablePort() {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        } catch (IOException e) {
            System.out.println("[ERROR]: Unable to obtain available port. Defaulting to 6000.");
            return 6000;
        }
    }

    private static Address parseAddress(String addressString) {
        String[] parts = addressString.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid address format. Use <ip>:<port>");
        }
        return new Address(parts[0], Integer.parseInt(parts[1]));
    }
}
