package storage;

import com.google.common.collect.ImmutableMap;
import org.junit.Test;
import worker.storage.TablePersister;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;

public class TablePersisterTests {

    private static final Collection<Map.Entry<String, String>> DATA = ImmutableMap.of(
            "a", "data",
            "b", "datab",
            "c", "datac").entrySet();
    @Test
    public void testIndexFile() throws IOException {
        new TablePersister(DATA).persist();
    }
}
