package konputer.kvdb;


import com.google.common.util.concurrent.Striped;

import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

public class Db implements Lookup, AutoCloseable, KvStore {
    private static final int LOCK_COUNT = 512;
    PersistentStore store = new PersistentStore();
    MemTablePersistor persistor = new MemTablePersistor(store);
    MemStore storeMem = new MemStore(persistor);
    Striped<Lock> locks = Striped.lock(LOCK_COUNT);



    @Override
    public ValueHolder get(String key) {
        try {
            ValueHolder value = storeMem.get(key);
            if (value != null) {
                return value;
            }
            return store.get(key);
        }catch (Exception e) {
            throw new RuntimeException("Error getting value for key: " + key, e);
        }
    }

    public byte[] getSimple(String key) {
        ValueHolder value = get(key);
        if (value != null) {
            return value.value();
        }
        return null;
    }


    public void set(String key, byte[] value) {
        set(key, new ValueHolder(value));
    }

    public void set(String key, ValueHolder value) {
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
        try{
            if(Arrays.equals(get(key).value(), expected)) {
                set(key, newVal);
                return true;
            }
        }finally {
            l.unlock();
        }
        return false;
    }

    @Override
    public void remove(String key) {
        set(key, ValueHolder.tombstone());
    }


    @Override
    public void close() {
        persistor.shutdown();
        persistor.close();
        store.close();
    }
}
