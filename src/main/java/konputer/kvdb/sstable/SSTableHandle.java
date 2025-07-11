package konputer.kvdb.sstable;

import konputer.kvdb.Lookup;
import konputer.kvdb.MemTable;
import konputer.kvdb.ValueHolder;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public final class SSTableHandle implements Closeable, Compactable, Lookup {

    public static final int BLOCK_SIZE = 1024 * 8; // 8kiBprivate final File file;
    private final File file;
    private final SSTableHeader header;
    private final FileChannel is;
    private final NavigableMap<String, Long> keyOffsets;
    private final long fileEnd;

    public SSTableHandle(File file, FileChannel raf, SSTableHeader header,
                         NavigableMap<String, Long> keyOffsets) {
        this.file = file;
        fileEnd = file.length();
        this.is = raf;
        this.header = header;
        this.keyOffsets = keyOffsets;
    }

    public static SSTableHandle create(File file, SSTableHeader header, NavigableMap<String, Long> ketOffsets) throws IOException {
        return new SSTableHandle(file, FileChannel.open(file.toPath(), StandardOpenOption.READ), header, ketOffsets);
    }


    public static SSTableHandle fromFile(File file) throws IOException {
        DataInputStream is = new DataInputStream(new BufferedInputStream(new FileInputStream(file)));
        int tblId = is.readInt();
        int maxTxId = is.readInt();
        int size = is.readInt();
        try (
                ObjectInputStream ois = new ObjectInputStream((new FileInputStream(file)));
        ) {
            @SuppressWarnings("unchecked") NavigableMap<String, Long> keyOffsets = (TreeMap<String, Long>) ois.readObject();
            return SSTableHandle.create(file, new SSTableHeader(tblId, maxTxId, size), keyOffsets);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public static SSTableHandle writeMemTable(MemTable memtable, File file, int tblId) throws IOException {
        //TODO: use something like Apache Avro for better serialization that supports schema evolution
        try (SSTableContentBuilder builder = new SSTableContentBuilder(file, new SSTableHeader(tblId, memtable.getCurrentTxId(), memtable.size()))) {
            memtable.serialize(builder);
            return builder.build();
        }
    }

    private long beginSearchOffset(String key) {
        // This method is used to find the offset of the key in the SSTable
        // It uses binary search on the keyOffsets map to find the closest key
        var entry = keyOffsets.floorEntry(key);
        if (entry != null) {
            return entry.getValue();
        }
        return -1;
    }

    private long endSearchOffset(String key) {
        // This method is used to find the end offset of the key in the SSTable
        // It uses binary search on the keyOffsets map to find the next key
        var entry = keyOffsets.higherEntry(key);
        if (entry != null) {
            return entry.getValue();
        }
        return -1;


    }


    @Override
    public ValueHolder get(String key) throws Exception {
        long endOffsetRaw = endSearchOffset(key);
        long endOffset = endOffsetRaw == -1 ? (fileEnd) : endOffsetRaw;
        long beginOffset = beginSearchOffset(key);
        if (beginOffset == -1) {
            return null; // empty or not found
        }
        ByteBuffer buf = ByteBuffer.allocate((int)(endOffset - beginOffset));
        is.read(buf, beginOffset);
        for (long currentOffset = 0; currentOffset < endOffset - beginOffset; ) {

            buf.position((int)currentOffset);
            int keyLength = buf.getInt();
            byte[] keyBytes = new byte[keyLength];
            buf.get(keyBytes);
            String readKey = new String(keyBytes, StandardCharsets.UTF_8);
            int valueLength = buf.getInt();
            if (!Objects.equals(key, readKey)) {
                currentOffset = buf.position() + valueLength;
            } else {
                byte[] value = new byte[valueLength];
                buf.get(value);
                return new ValueHolder(0, value);
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        is.close();
    }

    public File file() {
        return file;
    }
    public SSTableHeader header() {
        return header;
    }

    public long getSize() {
        return header.size();
    }
}
