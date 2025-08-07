package konputer.kvdb;

import java.util.concurrent.atomic.AtomicLong;

public class SnapshotManager {

    private AtomicLong snapshotIdCounter = new AtomicLong(0);

    public SnapshotManager(long snapshotIdCounter) {
        this.snapshotIdCounter.set(snapshotIdCounter);
    }

    public long doSnapshot() {
        return snapshotIdCounter.getAndIncrement();
    }

    public long currentSnapshotId() {
        return snapshotIdCounter.get();
    }



}
