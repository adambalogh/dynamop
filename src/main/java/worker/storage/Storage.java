package worker.storage;

import java.io.IOException;
import java.util.Optional;

public interface Storage {
    Optional<String> set(String key, String value) throws IOException;

    Optional<String> get(String key) throws IOException;
}
