package konputer.kvdb;

import java.util.Iterator;

public interface KvStore extends ReadOnlyKvStore {

    void set(String key, byte[] value);


    boolean cas(String key, byte[] newVal, byte[] expected);

    void remove(String key);

}
