package worker;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import worker.core.WorkerService;
import worker.discovery.ConsulClient;
import worker.discovery.ServiceWatcher;
import worker.ring.EventListenerAdapter;
import worker.ring.Ring;
import worker.routing.RoutingWorkerService;

import java.io.IOException;
import java.util.UUID;
import java.util.logging.Logger;

/**
 * Created by adambalogh.
 */
public class WorkerServer {

    private static final Logger logger = Logger.getLogger(WorkerServer.class.getName());

    private static final String SERVICE_NAME = "dynamo-worker";

    private Server server;
    private final int port;
    private final String serviceId = UUID.randomUUID().toString();

    private final RoutingWorkerService workerService;

    private final ConsulClient consulClient;
    private final ServiceWatcher serviceWatcher;
    private final Thread serviceWatcherThread;
    private final Ring ring = new Ring();

    public WorkerServer(int port) {
        this.port = port;
        this.consulClient = new ConsulClient();
        this.workerService = new RoutingWorkerService(serviceId, ring, new WorkerService());
        this.serviceWatcher = new ServiceWatcher(
                SERVICE_NAME, new EventListenerAdapter(ring.newEventListener()));
        this.serviceWatcherThread = new Thread(this.serviceWatcher);
    }

    private void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(workerService)
                .build()
                .start();
        logger.info("Server started, listening on " + port);

        consulClient.register(port, SERVICE_NAME, serviceId);
        logger.info("Server registered with Consul");

        this.serviceWatcherThread.start();
        logger.info("Started service watcher thread");

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                // Use stderr here since the logger may have been reset by its JVM shutdown hook.
                System.err.println("*** shutting down gRPC server since JVM is shutting down");
                WorkerServer.this.stop();
                System.err.println("*** server shut down");
            }
        });
    }

    private void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

    /**
     * Await termination on the main thread since the grpc library uses daemon threads.
     */
    private void blockUntilShutdown() throws InterruptedException {
        if (server != null) {
            server.awaitTermination();
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = Integer.parseInt(args[0]);
        final WorkerServer server = new WorkerServer(port);

        server.start();
        server.blockUntilShutdown();
    }
}
