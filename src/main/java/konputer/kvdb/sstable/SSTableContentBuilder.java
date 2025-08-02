package konputer.kvdb.sstable;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import konputer.kvdb.ValueHolder;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;


public class SSTableContentBuilder implements Closeable {
    private final SSTableHeader header;
    private final DataOutputStream os;
    private final File file;
    private final FileOutputStream fos;

    TreeMap<String, Long> keyOffsets = new TreeMap<>();
    BloomFilter<String> bloomFilter;

    public SSTableContentBuilder(File f, SSTableHeader header) throws IOException {
        this.header = header;
        this.file = f;
        fos = new FileOutputStream(this.file, false);
        this.os = new DataOutputStream(new BufferedOutputStream(fos));
        bloomFilter = BloomFilter.create(Funnels.stringFunnel(StandardCharsets.UTF_8), 10000);
        writeHeader();
    }

    private long currentBlockSize = Long.MAX_VALUE;

    public void writeKv(String key, ValueHolder value) throws IOException {

        this.bloomFilter.put(key);
        if (currentBlockSize >= SSTableHandle.BLOCK_SIZE) {
            os.flush();
            keyOffsets.put(key, fos.getChannel().position());
            currentBlockSize = 0;
        }
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        this.os.writeInt(keyBytes.length);
        this.os.write(keyBytes);
        long valLen = value.serialize(os);
        currentBlockSize += Integer.BYTES + key.length() + valLen;

    }

    private void writeHeader() throws IOException {
        this.header.serialize(this.os);
    }

    public SSTableHandle build() throws IOException {
        os.flush();
        File indexf = new File(file.getAbsolutePath() + ".index");
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(indexf))) {
            oos.writeObject(keyOffsets);
            oos.writeObject(bloomFilter);
        }

        return SSTableHandle.create(file, header, keyOffsets, bloomFilter);
    }


    @Override
    public void close() throws IOException {
        this.os.close();
    }
}

