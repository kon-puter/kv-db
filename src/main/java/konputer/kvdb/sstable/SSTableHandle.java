package konputer.kvdb.sstable;

import konputer.kvdb.Lookup;
import konputer.kvdb.MemTable;
import konputer.kvdb.ValueHolder;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.*;

import org.jooq.lambda.*;

public final class SSTableHandle implements Closeable, CompactableLookup, Compactable, Lookup {

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

    ByteBuffer getBlockAt(long beginOffset, long endOffset) {
        if (beginOffset < 0 || endOffset > fileEnd || beginOffset >= endOffset) {
            throw new IllegalArgumentException("Invalid offsets: beginOffset=" + beginOffset + ", endOffset=" + endOffset);
        }
        ByteBuffer buf = ByteBuffer.allocate((int) (endOffset - beginOffset));
        try {
            is.read(buf, beginOffset);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return buf;
    }

    @Override
    public List<Iterator<ByteBuffer>> getBlocks() {
        return List.of(
                Seq.seq(keyOffsets.values())
                        .window(0, 1)
                        .map(window ->
                                getBlockAt(window.nthValue(0).orElseThrow(), window.nthValue(1).orElse(fileEnd))
                        )
                        .iterator()
        );
    }

    public static class RowAwareBlock {

        private ByteBuffer block;
        boolean valueNext = false;

        public RowAwareBlock(ByteBuffer block) {
            this.block = block;
            block.position(0);
        }

        public ByteBuffer getBlock() {
            return block;
        }

        boolean hasMore() {
            return block.hasRemaining();
        }

        public void setBlock(ByteBuffer block) {
            this.block = block;
            valueNext = false;
            block.position(0);
        }

        public Iterator<Row> rowIterator(){
            return new Iterator<Row>() {
                @Override
                public boolean hasNext() {
                    return hasMore();
                }

                @Override
                public Row next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException("No more rows in the block");
                    }
                    String key = nextKey();
                    byte[] value = nextValue();
                    return new Row(key, value);
                }
            };
        }

        public String nextKey(){
            if (valueNext) {
                throw new IllegalStateException("nextKey() must be called before nextValue() or skipValue()");
            }

            int keyLength = block.getInt();
            byte[] keyBytes = new byte[keyLength];
            block.get(keyBytes);
            valueNext = true;
            return new String(keyBytes, StandardCharsets.UTF_8);
        }

        public long skipValue(){
            if (!valueNext) {
                throw new IllegalStateException("nextKey() must be called before skipValue()");
            }

            int valueLength = block.getInt();
            int currentOffset = block.position() + valueLength;
            block.position(currentOffset);
            valueNext = false;
            return currentOffset;
        }

        public byte[] nextValue() {
            if (!valueNext) {
                throw new IllegalStateException("nextKey() must be called before nextValue()");
            }
            int valueLength = block.getInt();
            byte[] value = new byte[valueLength];
            block.get(value);
            valueNext = false;
            return value;
        }

    }

    @Override
    public ValueHolder get(String key) throws Exception {
        long endOffsetRaw = endSearchOffset(key);
        long endOffset = endOffsetRaw == -1 ? (fileEnd) : endOffsetRaw;
        long beginOffset = beginSearchOffset(key);
        if (beginOffset == -1) {
            return null; // empty or not found
        }

        ByteBuffer buf = getBlockAt(beginOffset, endOffset);
        RowAwareBlock rowAwareBlock = new RowAwareBlock(buf);

        while(rowAwareBlock.hasMore()) {
            String currentKey = rowAwareBlock.nextKey();
            if (currentKey.equals(key)) {
                byte[] value = rowAwareBlock.nextValue();
                return new ValueHolder(0, value);
            } else {
                rowAwareBlock.skipValue(); // Skip the value if the key does not match
            }
        }

        return null;
    }

    @Override
    public void close() throws IOException {
        is.close();
    }

    @Override
    public void supersededNotification() {
        try {
            close();
            Files.deleteIfExists(file.toPath());
            Files.deleteIfExists(Path.of( file.toPath() + ".index"));
        }catch (Throwable e){
            throw new RuntimeException("Error deleting SSTableHandle", e);
        }
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
