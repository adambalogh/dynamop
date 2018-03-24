package worker;

import com.google.common.collect.Lists;
import com.orbitz.consul.model.catalog.CatalogService;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import worker.discovery.ConsulClient;
import worker.discovery.ServiceWatcher;
import worker.ring.Node;
import worker.ring.Ring;

import java.io.IOException;
import java.util.List;
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

    private final WorkerService workerService;

    private final ConsulClient consulClient;
    private final ServiceWatcher serviceWatcher;
    private final Thread serviceWatcherThread;

    public WorkerServer(int port, ConsulClient consulClient) {
        this.port = port;
        this.consulClient = consulClient;

        this.workerService = new WorkerService(serviceId, new Ring());

        this.serviceWatcher = new ServiceWatcher(
                SERVICE_NAME,
                new ServiceWatcherCallback(workerService.getServiceDiscoveryListener()));
        this.serviceWatcherThread = new Thread(this.serviceWatcher);
    }

    private void start() throws IOException {
        server = ServerBuilder.forPort(port)
                .addService(workerService)
                .build()
                .start();
        logger.info("Server started, listening on " + port);

        consulClient.register(port, SERVICE_NAME, UUID.randomUUID().toString());
        logger.info("Server registered with Consul");

        logger.info("Starting service watcher");
        this.serviceWatcherThread.start();

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

    private class ServiceWatcherCallback implements ServiceWatcher.Callback {
        private final WorkerService.ServiceDiscoveryListener listener;

        private final List<Node> lastNodesAlive = Lists.newArrayList();

        public ServiceWatcherCallback(WorkerService.ServiceDiscoveryListener listener) {
            this.listener = listener;
        }

        public void onServices(List<CatalogService> services) {
            List<Node> currentNodesAlive = Lists.newArrayList();

            for (CatalogService service : services) {
                Node node = new Node(service.getServiceAddress(), service.getServicePort(), service.getServiceId());
                currentNodesAlive.add(node);
                if (!lastNodesAlive.contains(node)) {
                    listener.onNodeJoin(node);
                }
            }

            for (Node node : lastNodesAlive) {
                if (!currentNodesAlive.contains(node)) {
                    listener.onNodeLeave(node);
                }
            }

            lastNodesAlive.clear();
            lastNodesAlive.addAll(currentNodesAlive);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        int port = Integer.parseInt(args[0]);
        ConsulClient consulClient = new ConsulClient();
        final WorkerServer server = new WorkerServer(port, consulClient);
        server.start();
        server.blockUntilShutdown();
    }
}
