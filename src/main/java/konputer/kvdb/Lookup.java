package konputer.kvdb;

import konputer.kvdb.dtos.ValueHolder;

public interface Lookup {
    ValueHolder get(String key) throws Exception;
}
