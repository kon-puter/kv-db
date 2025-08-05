package konputer.kvdb;

import konputer.kvdb.sstable.Row;

import java.util.Iterator;

public interface Lookup {
    ValueHolder get(String key) throws Exception;
}
