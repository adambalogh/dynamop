package worker.storage;

import java.io.*;
import java.util.Optional;

public class DiskStorage implements Storage {

    private static final int MAX_CACHE_SIZE = 10_000;

    private final Cache cache;

    public DiskStorage(Cache cache) {
        this.cache = cache;
    }

    public Optional<String> set(String key, String value) throws IOException {
        Optional<String> ret = cache.set(key, value);
        if (cache.size() >= MAX_CACHE_SIZE) {
            new TablePersister(cache.getSortedEntries()).persist();
            cache.clear();
        }
        return ret;
    }

    public Optional<String> get(String key) {
        Optional<String> val = cache.get(key);
        if (val.isPresent()) {
            return val;
        }

        return Optional.empty();
    }
}
