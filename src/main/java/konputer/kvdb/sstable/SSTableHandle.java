package konputer.kvdb.sstable;

import konputer.kvdb.Lookup;
import konputer.kvdb.MemTable;
import konputer.kvdb.ValueHolder;

import java.io.*;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.StandardOpenOption;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public final class SSTableHandle implements Closeable, Compactable, Lookup {

    public static final int BLOCK_SIZE = 1024 * 8; // 8kiBprivate final File file;
    private final MappedByteBuffer is;
    private final FileChannel fileChannel;
    private final File file;
    private final SSTableHeader header;
    private final NavigableMap<String, Long> keyOffsets;
    private final long fileEnd;

    public SSTableHandle(File file, MappedByteBuffer is, FileChannel fileChannel, SSTableHeader header,
                         NavigableMap<String, Long> keyOffsets) {
        this.file = file;
        fileEnd = file.length();
        this.is = is;
        this.fileChannel = fileChannel;
        this.header = header;
        this.keyOffsets = keyOffsets;
    }

    public static SSTableHandle create(File file, SSTableHeader header, NavigableMap<String, Long> ketOffsets) throws IOException {
        FileChannel fc = FileChannel.open(file.toPath(), StandardOpenOption.READ);
        MappedByteBuffer is = fc.map(FileChannel.MapMode.READ_ONLY, 0, file.length());
        return new SSTableHandle(file, is, fc, header, ketOffsets);
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
        for (long currentOffset = beginSearchOffset(key); currentOffset < endOffset; ) {
            if (currentOffset == -1) {
                return null; // empty or not found
            }
            is.position((int) currentOffset);



            int keyLength = is.getInt();
            byte[] keyBytes = new byte[keyLength];
            is.get(keyBytes);
            String readKey = new String(keyBytes, StandardCharsets.UTF_8);
            int valueLength = is.getInt();
            if (!Objects.equals(key, readKey)) {
                currentOffset = is.position() + valueLength;
            } else {
                byte[] value = new byte[valueLength];
                is.get(value);
                return new ValueHolder(0, value);
            }
        }
        return null;
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
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
