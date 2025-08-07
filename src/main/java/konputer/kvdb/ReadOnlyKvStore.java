package konputer.kvdb;

import konputer.kvdb.dtos.ValueHolder;

public interface ReadOnlyKvStore {
    ValueHolder get(String key);

    boolean containsKey(String key);
}
