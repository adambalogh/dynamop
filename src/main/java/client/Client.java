package client;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.StatusRuntimeException;
import worker.grpc.*;

import java.util.Scanner;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Created by adambalogh.
 */
public class Client {
    private static final Logger logger = Logger.getLogger(Client.class.getName());

    private final ManagedChannel channel;
    private final WorkerGrpc.WorkerBlockingStub blockingStub;

    /** Construct client connecting to HelloWorld server at {@code host:port}. */
    public Client(String host, int port) {
        this(ManagedChannelBuilder.forAddress(host, port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext(true)
                .build());
    }

    /** Construct client for accessing RouteGuide server using the existing channel. */
    Client(ManagedChannel channel) {
        this.channel = channel;
        blockingStub = WorkerGrpc.newBlockingStub(channel);
        logger.info("Connected to server");
    }

    public void shutdown() throws InterruptedException {
        channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
    }

    public void get(String key) {
        logger.info("Will try to get " + key + " ...");
        GetRequest request = GetRequest.newBuilder().setKey(key).build();
        GetResponse response;
        try {
            response = blockingStub.get(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Value: " + response.getValue());
    }

    public void set(String key, String value) {
        logger.info("Will try to set " + key + " to " + value + " ...");
        SetRequest request = SetRequest.newBuilder()
                .setKey(key)
                .setValue(value)
                .build();
        SetResponse response;
        try {
            response = blockingStub.set(request);
        } catch (StatusRuntimeException e) {
            logger.log(Level.WARNING, "RPC failed: {0}", e.getStatus());
            return;
        }
        logger.info("Previous: " + response.getValue());
    }

    public static void main(String[] args) throws Exception {
        Client client = new Client("localhost", 50051);
        Scanner scanner = new Scanner(System.in);
        try {

            while (true) {
                System.out.println("Enter command...");
                String command = scanner.next().toLowerCase();
                if (command.equals("set")) {
                    System.out.println("enter key");
                    String key = scanner.next();
                    System.out.println("enter value");
                    String value = scanner.next();
                    client.set(key, value);
                } else {
                    System.out.println("enter key");
                    String key = scanner.next();
                    client.get(key);
                }
            }

        } finally {
            client.shutdown();
        }
    }
}
