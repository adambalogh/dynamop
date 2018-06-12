package worker.storage;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;

public class MemoryStorage {
    private final SortedMap<String, String> storage = new ConcurrentSkipListMap<>();

    public Optional<String> set(String key, String value) {
        return Optional.ofNullable(storage.put(key, value));
    }

    public Optional<String> get(String key) {
        return Optional.ofNullable(storage.get(key));
    }

    public Collection<Map.Entry<String, String>> getSortedEntries() {
        return storage.entrySet();
    }

    public int size() {
        return storage.size();
    }

    public void clear() {
        storage.clear();
    }
}
