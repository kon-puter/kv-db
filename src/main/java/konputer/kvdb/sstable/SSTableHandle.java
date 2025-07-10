package konputer.kvdb.sstable;

import konputer.kvdb.Lookup;
import konputer.kvdb.MemTable;
import konputer.kvdb.ValueHolder;

import java.io.*;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public record SSTableHandle(File file, RandomAccessFile is, SSTableHeader header,
                            NavigableMap<String, Long> keyOffsets) implements Closeable, Compactable, Lookup {

    public static final int BLOCK_SIZE = 1024 * 8; // 8kiB

    public SSTableHandle(File file, SSTableHeader header, NavigableMap<String, Long> ketOffsets) throws FileNotFoundException {
        this(file, new RandomAccessFile(file, "r"), header, ketOffsets);
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
            return new SSTableHandle(file, new SSTableHeader(tblId, maxTxId, size), keyOffsets);
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
    long endOffset = endOffsetRaw == -1 ? is.length() : endOffsetRaw;
    for(long currentOffset = beginSearchOffset(key); currentOffset < endOffset; ) {
        if(currentOffset == -1) {
            return null; // empty or not found
        }
        is.seek(currentOffset);
        
        int keyLength = is.readInt();
        String readKey = is.readUTF();
        int valueLength = is.readInt();
        if (!Objects.equals(key, readKey)) {
            currentOffset = is.getChannel().position() + valueLength;
        }else{
            byte[] value = new byte[valueLength];
            is.readFully(value);
            return new ValueHolder(0, value);
        }
    }
    return null;
}

@Override
public void close() throws IOException {
    is.close();
}

public long getSize() {
    return header.size();
}
}
