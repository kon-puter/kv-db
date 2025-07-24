package konputer.kvdb.sstable;

import konputer.kvdb.Lookup;

public interface CompactableLookup extends Compactable, Lookup, AutoCloseable {

}
