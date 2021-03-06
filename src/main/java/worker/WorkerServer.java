package worker;

import com.orbitz.consul.Consul;
import com.orbitz.consul.ConsulException;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import worker.core.WorkerService;
import worker.discovery.ConsulClient;
import worker.discovery.ServiceWatcher;
import worker.health.HealthCheckService;
import worker.ring.EventListenerAdapter;
import worker.ring.Ring;
import worker.routing.RoutingWorkerService;
import worker.storage.DiskStorage;
import worker.storage.Cache;

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
    private final Ring ring = new Ring();

    private final HealthCheckService healthCheckService;

    public WorkerServer(int port, Consul consul) {
        this.port = port;
        this.consulClient = new ConsulClient(consul);
        this.workerService = new RoutingWorkerService(serviceId,
                ring,
                new WorkerService(new DiskStorage(new Cache())));
        this.serviceWatcher = new ServiceWatcher(
                consul,
                SERVICE_NAME,
                new EventListenerAdapter(ring.newEventListener()));
        this.healthCheckService = new HealthCheckService();
    }

    private void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(workerService)
                .addService(healthCheckService)
                .build()
                .start();
        logger.info("Server started, listening on " + port);

        consulClient.register(port, SERVICE_NAME, serviceId);
        logger.info("Server registered with Consul");

        serviceWatcher.start();
        logger.info("Started service watcher");

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
        serviceWatcher.stop();
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

        Consul consul;
        try {
            consul = Consul.builder().build(); // connect to Consul on localhost
        } catch (ConsulException ce) {
            logger.warning("Failed to connect to Consul, terminating");
            throw ce;
        }

        final WorkerServer server = new WorkerServer(port, consul);
        server.start();
        server.blockUntilShutdown();
    }
}
