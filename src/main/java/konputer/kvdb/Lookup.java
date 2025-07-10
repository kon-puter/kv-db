package konputer.kvdb;

public interface Lookup {
    ValueHolder get(String key) throws Exception;
}
