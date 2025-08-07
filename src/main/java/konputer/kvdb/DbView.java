package konputer.kvdb;

import konputer.kvdb.sstable.Row;

import java.util.Iterator;

public class DbView  implements ReadOnlyKvStore, AutoCloseable{
    private final long snapshotId;
    private Db db;

    public DbView(long snapshotId, Db db) {
        this.snapshotId = snapshotId;
        this.db = db;
    }

    @Override
    public ValueHolder get(String key) {
        Iterator<Row> it = db.getRange(new TaggedKey(key, 0), new TaggedKey(key, snapshotId));
        if(it.hasNext()){
            return it.next().value();
        }
        return null;
    }

    @Override
    public boolean containsKey(String key) {
        return get(key) != null;
    }



    @Override
    public void close() {
        db = null;
    }
}
