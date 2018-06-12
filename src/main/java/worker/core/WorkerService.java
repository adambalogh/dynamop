package worker.core;

import io.grpc.stub.StreamObserver;
import worker.grpc.*;
import worker.storage.Storage;

import java.util.Optional;

public class WorkerService extends WorkerGrpc.WorkerImplBase {
    private final Storage storage;

    public WorkerService(Storage storage) {
        this.storage = storage;
    }

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseStream) {
        try {
            Optional<String> value = storage.get(request.getKey());

            GetResponse.Builder builder = GetResponse.newBuilder()
                    .setStatus(value.isPresent() ? Status.OK : Status.FAILED);
            value.ifPresent((val) -> builder.setValue(val));

            responseStream.onNext(builder.build());
            responseStream.onCompleted();
        } catch (Exception e) {
            GetResponse response = GetResponse.newBuilder().setStatus(Status.FAILED).build();
            responseStream.onNext(response);
            responseStream.onCompleted();
        }
    }

    @Override
    public void set(SetRequest request, StreamObserver<SetResponse> responseStream) {
        try {
            Optional<String> previous = storage.set(request.getKey(), request.getValue());

            SetResponse.Builder builder = SetResponse.newBuilder().setStatus(Status.OK);
            previous.ifPresent((prev) -> builder.setValue(prev));

            responseStream.onNext(builder.build());
            responseStream.onCompleted();
        } catch (Exception e) {
            SetResponse response = SetResponse.newBuilder().setStatus(Status.FAILED).build();
            responseStream.onNext(response);
            responseStream.onCompleted();
        }
    }
}
