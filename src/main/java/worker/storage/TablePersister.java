package worker.storage;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import storage.file.EntryInfo;
import storage.file.Index;

import java.io.*;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class TablePersister {
    private static final String FILE_PATH = "/usr/local/dynamop/";

    private final Collection<Map.Entry<String, String>> data;

    public TablePersister(Collection<Map.Entry<String, String>> data) {
        this.data = data;
    }

    public void persist() throws IOException {
        writeIndex();
        writeData();
    }

    private void writeIndex() throws IOException {
        Index index = buildIndex();
        FileOutputStream outputStream = new FileOutputStream(getIndexFile());
        index.writeTo(outputStream);
    }

    private Index buildIndex() {
        long offset = 0;
        List<EntryInfo> entryInfoList = Lists.newArrayList();
        for (Map.Entry<String, String> entry : data) {
            entryInfoList.add(EntryInfo.newBuilder()
                    .setKeyLength(entry.getKey().length())
                    .setKeyOffset(offset)
                    .build());
            offset += entry.getKey().length() + entry.getValue().length();
        }

        return Index.newBuilder()
                .setNumEntries(data.size())
                .addAllEntries(entryInfoList)
                .build();
    }

    private void writeData() throws IOException {
       try (BufferedWriter writer = Files.newWriter(getDataFile(), Charset.defaultCharset())) {
           for (Map.Entry<String, String> entry : data) {
               writer.write(entry.getKey());
               writer.write(entry.getValue());
           }
       }
    }

    private File getDataFile() {
        return new File(FILE_PATH + "data_file");
    }

    private File getIndexFile() {
        return new File(FILE_PATH + "index_file");
    }
}
