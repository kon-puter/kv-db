package konputer.kvdb;

import konputer.kvdb.sstable.SSTableContentBuilder;

import java.io.IOException;
import java.util.Arrays;
import java.util.TreeMap;

public class MemTable implements KvStore {
    private final TreeMap<String, ValueHolder> store;
    private final TransactionManager tm;
    private int maxTxId = 0;
    private long sizeBytes = 0;

    public MemTable(TransactionManager tm) {
        this.store = new TreeMap<>();
        this.tm = tm;
    }

    @Override
    public void set(String key, byte[] value) {
        maxTxId = tm.getCurrentTxId(); // txid is monotonically increasing
        sizeBytes += value.length + Integer.BYTES; // +4 for the length of the value
        ValueHolder old = store.put(key, new ValueHolder(maxTxId, value));
        if (old != null) {
            sizeBytes -= old.value().length;
            sizeBytes -= Integer.BYTES;
        }else {
            // If the key was not present, we need to account for the key length as well
            sizeBytes += key.length() + Integer.BYTES; // +4 for the length of the key
        }
    }

    @Override
    public ValueHolder get(String key) {
        return store.get(key);
    }

    @Override
    public boolean containsKey(String key) {
        return store.containsKey(key);
    }

    @Override
    public boolean cas(String key, byte[] newVal, byte[] expected) {
        byte[] oldVal = store.get(key).value();
        if (Arrays.equals(expected, oldVal)) {
            set(key, newVal);
            return true;
        }
        return false;
    }

    @Override
    public void remove(String key) {
        maxTxId = tm.getCurrentTxId();

        ValueHolder old = store.remove(key);
        sizeBytes -= old.value().length + Integer.BYTES; // -4 for the length of the value
        sizeBytes -= key.length() + Integer.BYTES; // -4 for the length of the key
    }

    @Override
    public long size() {
        return sizeBytes;
    }

    public void clear() {
        maxTxId = tm.getCurrentTxId();
        store.clear();
    }

    public void serialize(SSTableContentBuilder b) throws IOException {
        //write number of records for future use
        for (var entry : store.entrySet()) {
            b.writeKv(entry.getKey(), entry.getValue().value());
        }
    }

    public int getCurrentTxId() {
        return maxTxId;
    }
}
