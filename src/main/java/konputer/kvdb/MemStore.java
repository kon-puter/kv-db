package konputer.kvdb;

import konputer.kvdb.sstable.Row;
import org.jspecify.annotations.NonNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class MemStore implements AutoCloseable {
    public static final int MAX_MEMTABLE_SIZE = 1024 * 1024;

    private MemTable activeMemTable;
    private final MemTablePersistor persistor;
    private final SnapshotManager snapshotManager;
    public MemStore(MemTablePersistor persistor, SnapshotManager snapshotManager) {
        this.activeMemTable = new MemTable(snapshotManager);
        this.snapshotManager = snapshotManager;
        this.persistor = persistor;
    }

    public void set(String key, ValueHolder value) {
        // Implementation for setting a key-value pair in the database
        activeMemTable.set(key, value);
        if (activeMemTable.size() >= MAX_MEMTABLE_SIZE) {
            synchronized (this) {
                if (activeMemTable.size() >= MAX_MEMTABLE_SIZE) {
                    flush();
                }
            }
        }

    }

    public Iterator<Row> getRawRange(@NonNull TaggedKey from, @NonNull TaggedKey to) {
        // Implementation for getting a range of rows from the database
        return activeMemTable.getRawRange(from, to);
    }

    private void flush() {
        // Implementation for flushing the current memtable to persistent storage
        persistor.schedulePersist(activeMemTable);
        activeMemTable = new MemTable(snapshotManager);
    }

    public ValueHolder get(String key) throws Exception {
        // Implementation for getting a value by key from the database
        ValueHolder value = activeMemTable.get(key);
        if (value != null) {
            return value;
        }
        for (Iterator<MemTable> it = persistor.getNonCompleted(); it.hasNext(); ) {
            MemTable frozen = it.next();
            value = frozen.get(key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }


    @Override
    public void close() {
        persistor.close();

    }

}
