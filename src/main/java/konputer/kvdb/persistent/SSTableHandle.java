package konputer.kvdb.persistent;

import com.google.common.hash.BloomFilter;
import konputer.kvdb.*;
import konputer.kvdb.compaction.Compactable;
import konputer.kvdb.compaction.CompactableLookup;
import konputer.kvdb.dtos.Row;
import konputer.kvdb.dtos.TaggedKey;
import konputer.kvdb.dtos.ValueHolder;
import konputer.kvdb.memory.MemTable;
import konputer.kvdb.utils.RowTransformingIterable;
import org.jooq.lambda.Seq;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.NoSuchElementException;

public final class SSTableHandle implements Closeable, CompactableLookup, Compactable, Lookup {

    public static final int BLOCK_SIZE = 1024;
    private final File file;
    private final SSTableHeader header;
    private final FileChannel is;
    private final MappedByteBuffer isMap;
    private final NavigableMap<TaggedKey, Long> keyOffsets;
    private final long fileEnd;
    private final BloomFilter<String> bloomFilter;

    public SSTableHandle(File file, FileChannel raf, SSTableHeader header, NavigableMap<TaggedKey, Long> keyOffsets, BloomFilter<String> bloomFilter) throws IOException {
        this.file = file;
        fileEnd = file.length();
        this.is = raf;
        this.bloomFilter = bloomFilter;
        this.isMap = this.is.map(FileChannel.MapMode.READ_ONLY, 0, fileEnd);
        this.header = header;
        this.keyOffsets = keyOffsets;
    }

    public static SSTableHandle create(File file, SSTableHeader header, NavigableMap<TaggedKey, Long> ketOffsets, BloomFilter<String> bloomFilter) throws IOException {
        return new SSTableHandle(file, FileChannel.open(file.toPath(), StandardOpenOption.READ), header, ketOffsets, bloomFilter);
    }

    public static SSTableHandle writeMemTable(MemTable memtable, File file, int tblId) throws IOException {
        //TODO: use something like Apache Avro for better serialization that supports schema evolution
        try (SSTableContentBuilder builder = new SSTableContentBuilder(file, new SSTableHeader(tblId, memtable.size()))) {
            memtable.serialize(builder);
            return builder.build();
        }
    }

    private long beginSearchOffset(TaggedKey key) {
        // This method is used to find the offset of the key in the SSTable
        // It uses binary search on the keyOffsets map to find the closest key
        var entry = keyOffsets.floorEntry(key);
        if (entry != null) {
            return entry.getValue();
        }
        return -1;
    }

    private long endSearchOffset(TaggedKey key) {
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
        return isMap.slice((int) beginOffset, (int) (endOffset - beginOffset));
    }

    public Iterator<Row> getRawRange(TaggedKey from, TaggedKey to) {
        if (from.compareTo(to) > 0) {
            throw new IllegalArgumentException("from key must be less than or equal to to key");
        }

        return (new RowTransformingIterable(List.of(blockRangeIterator(from, to)))).iterator();
    }

    @Override
    public List<Iterator<ByteBuffer>> getRawBlocks(TaggedKey from, TaggedKey to) {
        return List.of(blockRangeIterator(from, to));
    }

    public Iterator<ByteBuffer> blockRangeIterator(TaggedKey from, TaggedKey to) {
        if (from.compareTo(to) > 0) {
            throw new IllegalArgumentException("from key must be less than or equal to to key");
        }

        return Seq.seq(keyOffsets.subMap(from, true, to, true).values()).window(0, 1).map(window -> getBlockAt(window.nthValue(0).orElseThrow(), window.nthValue(1).orElse(fileEnd))).iterator();

    }

    @Override
    public List<Iterator<ByteBuffer>> getBlocks() {
        return List.of(Seq.seq(keyOffsets.values()).window(0, 1).map(window -> getBlockAt(window.nthValue(0).orElseThrow(), window.nthValue(1).orElse(fileEnd))).iterator());
    }

    public static class RowAwareBlock {

        private final ByteBuffer block;
        boolean valueNext = false;

        public RowAwareBlock(ByteBuffer block) {
            this.block = block;
            block.position(0);
        }

        boolean hasMore() {
            return block.hasRemaining();
        }

        public Iterator<Row> rowIterator() {
            return new Iterator<>() {
                @Override
                public boolean hasNext() {
                    return hasMore();
                }

                @Override
                public Row next() {
                    if (!hasNext()) {
                        throw new NoSuchElementException("No more rows in the block");
                    }
                    return new Row(nextKey(), nextValue());
                }
            };
        }

        public TaggedKey nextKey() {
            if (valueNext) {
                throw new IllegalStateException("nextKey() must be called before nextValue() or skipValue()");
            }
            valueNext = true;
            return TaggedKey.deserialize(block);
        }

        public long skipValue() {
            if (!valueNext) {
                throw new IllegalStateException("nextKey() must be called before skipValue()");
            }

            int valueLength = block.getInt();
            int currentOffset = block.position() + Integer.max(valueLength, 0);
            block.position(currentOffset);
            valueNext = false;
            return currentOffset;
        }

        public ValueHolder nextValue() {
            if (!valueNext) {
                throw new IllegalStateException("nextKey() must be called before nextValue()");
            }

            return ValueHolder.deserialize(block);
        }

    }

    @Override
    public ValueHolder get(String key) throws Exception {

        if (!bloomFilter.mightContain(key)) {
            return null;
        }


        long endOffsetRaw = endSearchOffset(new TaggedKey(key, Long.MAX_VALUE));
        long endOffset = endOffsetRaw == -1 ? (fileEnd) : endOffsetRaw;
        long beginOffset = beginSearchOffset(new TaggedKey(key, 0));
        if (beginOffset == -1) {
            return null; // empty or not found
        }

        ByteBuffer buf = getBlockAt(beginOffset, endOffset);
        RowAwareBlock rowAwareBlock = new RowAwareBlock(buf);

        while (rowAwareBlock.hasMore()) {
            TaggedKey currentKey = rowAwareBlock.nextKey();
            if (currentKey.key().equals(key)) {
                return rowAwareBlock.nextValue();
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
            Files.deleteIfExists(Path.of(file.toPath() + ".index"));
        } catch (Throwable e) {
            throw new RuntimeException("Error deleting SSTableHandle", e);
        }
    }


    public long getSize() {
        return header.size();
    }
}
