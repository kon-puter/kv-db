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
    private final PersistentStore store = new PersistentStore();
    private final MemTablePersistor persistor = new MemTablePersistor(store);
    private final SnapshotManager snapshotManager = new SnapshotManager(1);
    private final MemStore storeMem = new MemStore(persistor, snapshotManager);
    private final Striped<Lock> locks = Striped.lock(LOCK_COUNT);


    @Override
    public ValueHolder get(@NonNull String key) {
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

    public byte[] getSimple(@NonNull String key) {
        ValueHolder value = get(key);
        if (value != null) {
            return value.value();
        }
        return null;
    }

    public Iterator<Row> getRange(@NonNull TaggedKey from, @NonNull TaggedKey to) {
        //Iterator that filters out tombstones and older entries with the same key
        return new Iterator<>() {
            private final Iterator<Row> inner = rawIterate(from, to);
            private Row prev = null;
            private Row peeked = null;
            private boolean seen = true;

            @Override
            public boolean hasNext() {
                if (!seen) {
                    return true;
                }
                while (inner.hasNext()) {
                    Row next = inner.next();
                    if (
                            next != null && next.value() != null && !next.value().isTombstone() &&
                                    (prev == null || !next.key().equals(prev.key()))
                    ) {
                        prev = peeked;
                        peeked = next;
                        seen = false;
                        return true;
                    }
                }
                return false;
            }

            @Override
            public Row next() {
                if (!hasNext()) {
                    throw new IllegalStateException("No more elements in the iterator");
                }
                seen = true;
                return peeked;
            }
        };
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

    public void set(@NonNull String key, byte[] value) {

        set(new TaggedKey(key, snapshotManager.currentSnapshotId()), new ValueHolder(value));
    }

    public void set(@NonNull TaggedKey key, @NonNull ValueHolder value) {
        storeMem.set(key, value);
    }

    @Override
    public boolean containsKey(@NonNull String key) {
        return getSimple(key) != null;
    }

    @Override
    public boolean cas(@NonNull String key, byte[] newVal, byte[] expected) {
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
    public void remove(@NonNull String key) {
        set(new TaggedKey(key, snapshotManager.currentSnapshotId()), ValueHolder.tombstone());
    }


    @Override
    public void close() {
        persistor.shutdown();
        persistor.close();
        store.close();
    }
}
