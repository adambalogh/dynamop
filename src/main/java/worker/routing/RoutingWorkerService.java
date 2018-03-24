package worker.routing;

import com.google.common.collect.Maps;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;
import worker.core.WorkerService;
import worker.grpc.*;
import worker.ring.ConsistentHash;
import worker.ring.Node;
import worker.ring.Ring;

import java.util.Map;
import java.util.logging.Logger;

/**
 * Created by adambalogh.
 */
public class RoutingWorkerService extends WorkerGrpc.WorkerImplBase {
    private static final Logger logger = Logger.getLogger(RoutingWorkerService.class.getName());

    private final String serviceId; // unique identifier accross all worker nodes
    private final Ring ring;

    private final WorkerService workerImpl;
    private final Map<String, WorkerGrpc.WorkerStub> futureStubs;

    public RoutingWorkerService(String serviceId, Ring ring, WorkerService workerImpl) {
        this.serviceId = serviceId;
        this.ring = ring;
        this.workerImpl = workerImpl;
        this.futureStubs = Maps.newConcurrentMap();
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseStream) {
        Node assignedNode = getAssignedNode(request.getKey());

        if (assignedNode.serviceId.equals(serviceId)) {
            logger.info("Executing get request locally");
            workerImpl.get(request, responseStream);
        } else {
            logger.info("Forwarding get request to: " + assignedNode.serviceId);
            getStub(assignedNode).get(request, new StreamObserver<GetResponse>() {
                @Override
                public void onNext(GetResponse response) {
                    responseStream.onNext(response);
                }

                @Override
                public void onError(Throwable t) {
                    responseStream.onNext(GetResponse.newBuilder().setStatus(Status.FAILED).build());
                }

                @Override
                public void onCompleted() {
                    responseStream.onCompleted();
                }
            });
        }
    }

    @Override
    public void set(SetRequest request, StreamObserver<SetResponse> responseStream) {
        Node assignedNode = getAssignedNode(request.getKey());
        logger.info("Assigned node: " + assignedNode.serviceId);

        if (assignedNode.serviceId.equals(serviceId)) {
            logger.info("Executing set request locally");
            workerImpl.set(request, responseStream);
        } else {
            logger.info("Forwarding set request to: " + assignedNode.serviceId);
            getStub(assignedNode).set(request, new StreamObserver<SetResponse>() {
                @Override
                public void onNext(SetResponse response) {
                    responseStream.onNext(response);
                }

                @Override
                public void onError(Throwable t) {
                    responseStream.onNext(SetResponse.newBuilder().setStatus(Status.FAILED).build());
                }

                @Override
                public void onCompleted() {
                    responseStream.onCompleted();
                }
            });
        }
    }

    private Node getAssignedNode(String key) {
        return ring.getAssignedNode(ConsistentHash.getBucket(key));
    }

    private WorkerGrpc.WorkerStub getStub(Node node) {
        return futureStubs.computeIfAbsent(node.serviceId, (requestedServiceId) -> {
            logger.info("Connecting to service: " + node.serviceId);
            return WorkerGrpc.newStub(ManagedChannelBuilder.forAddress(node.url, node.port)
                // Channels are secure by default (via SSL/TLS). For the example we disable TLS to avoid
                // needing certificates.
                .usePlaintext(true)
                .build());
        });
    }
}
