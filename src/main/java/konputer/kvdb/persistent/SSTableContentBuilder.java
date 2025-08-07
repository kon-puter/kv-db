package konputer.kvdb.persistent;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;
import konputer.kvdb.dtos.TaggedKey;
import konputer.kvdb.dtos.ValueHolder;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.TreeMap;


public class SSTableContentBuilder implements Closeable {
    private final SSTableHeader header;
    private final DataOutputStream os;
    private final File file;
    private final FileOutputStream fos;

    TreeMap<TaggedKey, Long> keyOffsets = new TreeMap<>();
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

    public void writeKv(TaggedKey key, ValueHolder value) throws IOException {
        this.bloomFilter.put(key.key());
        if (currentBlockSize >= SSTableHandle.BLOCK_SIZE) {
            os.flush();
            keyOffsets.put(key, fos.getChannel().position());
            currentBlockSize = 0;
        }
        long keyLen = key.serialize(os);
        long valLen = value.serialize(os);


        currentBlockSize += keyLen + valLen;
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

