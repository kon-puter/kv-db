package konputer.kvdb;

import konputer.kvdb.sstable.SSTableContentBuilder;

import java.io.IOException;
import java.util.Optional;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.LongAdder;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MemTable implements KvStore {
    private final ConcurrentSkipListMap<String, ValueHolder> store = new ConcurrentSkipListMap<>();
    private final LongAdder sizeBytes = new LongAdder();

    private final ReadWriteLock sizeLock = new ReentrantReadWriteLock();


    @Override
    public void set(String key, byte[] value) {
        sizeLock.readLock().lock();
        try{
            sizeBytes.add(value.length + Integer.BYTES); // +4 for the length of the value
            ValueHolder old = store.put(key, new ValueHolder(value));
            if (old != null) {
                sizeBytes.add(-old.value().length - Integer.BYTES); // -4 for the length of the value
            } else {
                // If the key was not present, we need to account for the key length as well
                sizeBytes.add(key.length() + Integer.BYTES);
            }
        }finally {
            sizeLock.readLock().unlock();
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
        //TODO: test adding outer condition
        sizeLock.readLock().lock();
        try
         {
            if (store.replace(key, new ValueHolder(expected), new ValueHolder(newVal))) {
                sizeBytes.add(newVal.length - expected.length); // Adjust size for the new value
                return true;
            }
        }finally {
            sizeLock.readLock().unlock();
        }
        return false;
    }

    @Override
    public void remove(String key) {

        ValueHolder old = store.remove(key);
        if(old == null){
            return;
        }
        //TODO: fix charset issues
        sizeBytes.add(-old.value().length - Integer.BYTES -key.length() - Integer.BYTES); // -4 for the length of the value
    }

    @Override
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
            b.writeKv(entry.getKey(), entry.getValue().value());
        }
    }
}
