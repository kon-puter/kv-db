package konputer.kvdb.memory;

import konputer.kvdb.persistent.PersistentStore;
import konputer.kvdb.dtos.Row;
import konputer.kvdb.dtos.TaggedKey;
import konputer.kvdb.persistent.SSTableHandle;
import org.jooq.lambda.Seq;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.VarHandle;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.google.common.base.Preconditions.checkState;

public class MemTablePersistor implements AutoCloseable {
    ExecutorService executor = Executors.newSingleThreadExecutor();
    private final ConcurrentLinkedQueue<MemTable> nonCompleted = new ConcurrentLinkedQueue<>();

    private final PersistentStore persistentStore;

    public MemTablePersistor(PersistentStore persistentStore) {
        this.persistentStore = persistentStore;
    }

    public void schedulePersist(MemTable memTable) {
        if (memTable == null) {
            throw new IllegalArgumentException("MemTable cannot be null");
        }
        nonCompleted.add(memTable);
        final int tblId = persistentStore.nextTblId();
        executor.submit(() -> {
            //is exactly the same as memTable in function parameter due to single thread executor
            MemTable toHandle = nonCompleted.peek();
            checkState(toHandle != null);
            try {
                SSTableHandle h = SSTableHandle.writeMemTable(toHandle, new File("tbl_" + tblId + ".sstable"), tblId);

                persistentStore.addSSTable(h);
                // could be reordered somehow CPU or JVM optimizations probably won't reorder this, but it's better to be safe
                // synchronization is overkill as having both SSTable and MemTable doesn't produce wrong results
                VarHandle.acquireFence();
                nonCompleted.remove();
            } catch (IOException e) {
                throw new RuntimeException("Failed to persist memtable", e);
            }
        });
    }


    public List<Iterator<Row>> getRawRange(TaggedKey from, TaggedKey to) {
        // This method is used to get a range of rows from the non-completed memtables
        return Seq.seq(nonCompleted)
                .map(memTable -> memTable.getRawRange(from, to))
                .toUnmodifiableList();
    }


    public Iterator<MemTable> getNonCompleted() {
        return nonCompleted.iterator();
    }

    public void shutdown() {
        executor.shutdown();
    }

    public void close() {
        shutdown();

        try {
            if (!executor.awaitTermination(60, java.util.concurrent.TimeUnit.SECONDS)) {
                executor.shutdownNow();
            }
        } catch (InterruptedException e) {
            executor.shutdownNow();
        }
        if (!nonCompleted.isEmpty()) {
            throw new RuntimeException("Failed to persist memtable, " + Thread.currentThread().isInterrupted());
        }
    }

}
