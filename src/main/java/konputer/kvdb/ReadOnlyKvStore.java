package konputer.kvdb;

public interface ReadOnlyKvStore {
    ValueHolder get(String key);
    boolean containsKey(String key);
}
