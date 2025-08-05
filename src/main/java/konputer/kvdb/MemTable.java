package konputer.kvdb;

import konputer.kvdb.sstable.Row;
import konputer.kvdb.sstable.SSTableContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemTable{
    private final ConcurrentSkipListMap<TaggedKey, ValueHolder> store = new ConcurrentSkipListMap<>();
    private final LongAdder sizeBytes = new LongAdder();
    private final ReadWriteLock sizeLock = new ReentrantReadWriteLock();

    private final SnapshotManager snapshotManager;

    public MemTable(SnapshotManager manager) {
        this.snapshotManager = manager;
    }

    public void set(String key, ValueHolder value) {
        sizeLock.readLock().lock();
        try{
            sizeBytes.add(value.length() + Integer.BYTES); // +4 for the length of the value
            ValueHolder old = store.put(snapshotManager.taggedKey(key), value);
            if (old != null) {
                sizeBytes.add(-old.length() - Integer.BYTES); // -4 for the length of the value
            } else {
                // If the key was not present, we need to account for the key length as well
                sizeBytes.add(key.length() + Integer.BYTES);
            }
        }finally {
            sizeLock.readLock().unlock();
        }
    }

    public ValueHolder get(String key) {

        var value = store.floorEntry(new TaggedKey(key, Long.MAX_VALUE));

        if(value == null) {
            return null;
        }

        if(value.getKey().key().equals(key)){
            return value.getValue();
        }

        return null;
    }

    public Iterator<Row> getRawRange(TaggedKey from, TaggedKey to){
        return store.subMap(from, true, to, true).entrySet()
                .stream()
                .map(e -> new Row(e.getKey(), e.getValue()))
                .iterator();
    }

    public long size() {
        sizeLock.writeLock().lock();
        try {
            return sizeBytes.sum();
        }finally {
            sizeLock.writeLock().unlock();
        }
    }

    public void serialize(SSTableContentBuilder b) throws IOException {
        //write number of records for future use
        for (var entry : store.entrySet()) {
            b.writeKv(entry.getKey(), entry.getValue());
        }
    }
}
