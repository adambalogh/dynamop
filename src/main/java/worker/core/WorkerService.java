package worker.core;

import com.google.common.collect.Maps;
import io.grpc.stub.StreamObserver;
import worker.grpc.*;

import java.util.Map;
import java.util.Optional;

public class WorkerService extends WorkerGrpc.WorkerImplBase {
    private final Map<String, String> storage = Maps.newConcurrentMap();

    @Override
    public void get(GetRequest request, StreamObserver<GetResponse> responseStream) {
        Optional<String> value = Optional.ofNullable(storage.get(request.getKey()));

        GetResponse.Builder builder = GetResponse.newBuilder()
                .setStatus(value.isPresent() ? Status.OK : Status.FAILED);
        value.ifPresent((val) -> builder.setValue(val));

        responseStream.onNext(builder.build());
        responseStream.onCompleted();
    }

    @Override
    public void set(SetRequest request, StreamObserver<SetResponse> responseStream) {
        Optional<String> previous = Optional.ofNullable(storage.put(request.getKey(), request.getValue()));

        SetResponse.Builder builder = SetResponse.newBuilder().setStatus(Status.OK);
        previous.ifPresent((prev) -> builder.setValue(prev));

        responseStream.onNext(builder.build());
        responseStream.onCompleted();
    }
}
