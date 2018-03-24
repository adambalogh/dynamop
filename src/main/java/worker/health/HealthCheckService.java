package worker.health;

import grpc.health.v1.HealthCheckRequest;
import grpc.health.v1.HealthCheckResponse;
import grpc.health.v1.HealthGrpc;
import io.grpc.stub.StreamObserver;

/**
 * Created by adambalogh.
 */
public class HealthCheckService extends HealthGrpc.HealthImplBase {
    @Override
    public void check(HealthCheckRequest request, StreamObserver<HealthCheckResponse> responseStream) {
        responseStream.onNext(HealthCheckResponse.newBuilder()
                .setStatus(HealthCheckResponse.ServingStatus.SERVING)
                .build());
        responseStream.onCompleted();
    }
}
