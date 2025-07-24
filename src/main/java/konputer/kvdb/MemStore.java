package konputer.kvdb;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;

public class MemStore implements AutoCloseable {
    public static final int MAX_MEMTABLE_SIZE = 1024 * 1024;

    TransactionManager tmanager;
    MemTable activeMemTable;
    private final MemTablePersistor persistor;

    public MemStore(MemTablePersistor persistor) {
        this.tmanager = new TransactionManager();
        this.activeMemTable = new MemTable(tmanager);
        this.persistor = persistor;
    }

    public void set(String key, byte[] value) {
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

    private void flush() {
        // Implementation for flushing the current memtable to persistent storage
        persistor.schedulePersist(activeMemTable);
        activeMemTable = new MemTable(tmanager);
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
