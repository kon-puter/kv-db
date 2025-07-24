package konputer.kvdb;

public class Db implements Lookup, AutoCloseable {
    PersistentStore store = new PersistentStore();
    MemTablePersistor persistor = new MemTablePersistor(store);
    MemStore storeMem = new MemStore(persistor);


    @Override
    public ValueHolder get(String key) throws Exception {
        ValueHolder value = storeMem.get(key);
        if (value != null) {
            return value;
        }
        return store.get(key);
    }


    public void set(String key, byte[] value) {
        storeMem.set(key, value);
    }

    @Override
    public void close() throws Exception {
        persistor.shutdown();

        persistor.close();
        store.close();
    }
}
