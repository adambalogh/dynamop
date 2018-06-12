package worker.storage;

import java.io.*;
import java.util.Optional;

public class DiskStorage implements Storage {

    private static final int MAX_CACHE_SIZE = 10_000;

    private final MemoryStorage memoryStorage;

    public DiskStorage(MemoryStorage memoryStorage) {
        this.memoryStorage = memoryStorage;
    }

    public Optional<String> set(String key, String value) throws IOException {
        Optional<String> ret = memoryStorage.set(key, value);
        if (memoryStorage.size() >= MAX_CACHE_SIZE) {
            new TablePersister(memoryStorage.getSortedEntries()).persist();
            memoryStorage.clear();
        }
        return ret;
    }

    public Optional<String> get(String key) {
        Optional<String> val = memoryStorage.get(key);
        if (val.isPresent()) {
            return val;
        }

        return Optional.empty();
    }
}
