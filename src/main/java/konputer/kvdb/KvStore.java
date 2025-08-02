package konputer.kvdb;

import java.util.Iterator;

public interface KvStore {

    void set(String key, byte[] value);

    ValueHolder get(String key);

    boolean containsKey(String key);

    boolean cas(String key, byte[] newVal, byte[] expected);

    void remove(String key);

}
