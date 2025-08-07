package konputer.kvdb;


import com.google.common.collect.Iterators;
import com.google.common.util.concurrent.Striped;
import konputer.kvdb.dtos.Row;
import konputer.kvdb.dtos.TaggedKey;
import konputer.kvdb.dtos.ValueHolder;
import konputer.kvdb.memory.MemStore;
import konputer.kvdb.memory.MemTablePersistor;
import konputer.kvdb.persistent.PersistentStore;
import org.jspecify.annotations.NonNull;

import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.stream.Stream;

public class Db implements Lookup, AutoCloseable, KvStore {
    private static final int LOCK_COUNT = 512;
    PersistentStore store = new PersistentStore();
    MemTablePersistor persistor = new MemTablePersistor(store);
    private final SnapshotManager snapshotManager = new SnapshotManager(1);
    MemStore storeMem = new MemStore(persistor, snapshotManager);
    Striped<Lock> locks = Striped.lock(LOCK_COUNT);


    @Override
    public ValueHolder get(String key) {
        try {
            ValueHolder value = storeMem.get(key);
            if (value != null) {
                return value;
            }
            return store.get(key);
        } catch (Exception e) {
            throw new RuntimeException("Error getting value for key: " + key, e);
        }
    }

    public DbView snapshot() {
        long snapshotId = snapshotManager.doSnapshot();

        return new DbView(snapshotId, this);
    }

    public byte[] getSimple(String key) {
        ValueHolder value = get(key);
        if (value != null) {
            return value.value();
        }
        return null;
    }

    public Iterator<Row> getRange(TaggedKey from, TaggedKey to) {
        return rawIterate(from, to);
    }

    public Iterator<Row> rawIterate(@NonNull TaggedKey from, @NonNull TaggedKey to) {
        Iterator<Row> activeMemTableIt = storeMem.getRawRange(from, to);
        List<Iterator<Row>> intermediateIterators = persistor.getRawRange(from, to);
        Iterator<Row> persistentIt = store.getRawRange(from, to);
        Stream<Iterator<Row>> allIterators = Stream.concat(
                Stream.concat(Stream.of(activeMemTableIt), intermediateIterators.stream()),
                Stream.of(persistentIt)
        );
        return Iterators.mergeSorted(allIterators.toList(),
                Comparator.naturalOrder());
    }

    public void set(String key, byte[] value) {
        set(new TaggedKey(key, snapshotManager.currentSnapshotId()), new ValueHolder(value));
    }

    public void set(TaggedKey key, ValueHolder value) {
        storeMem.set(key, value);
    }

    @Override
    public boolean containsKey(String key) {
        return getSimple(key) != null;
    }

    @Override
    public boolean cas(String key, byte[] newVal, byte[] expected) {
        //TODO test adding outer condition
        Lock l = locks.get(key);
        l.lock();
        try {
            if (Arrays.equals(get(key).value(), expected)) {
                set(key, newVal);
                return true;
            }
        } finally {
            l.unlock();
        }
        return false;
    }

    @Override
    public void remove(String key) {
        set(new TaggedKey(key, snapshotManager.currentSnapshotId()), ValueHolder.tombstone());
    }


    @Override
    public void close() {
        persistor.shutdown();
        persistor.close();
        store.close();
    }
}
