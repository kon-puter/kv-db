package konputer.kvdb.sstable;

import konputer.kvdb.TaggedKey;
import konputer.kvdb.ValueHolder;

public record Row(
        TaggedKey key,
        ValueHolder value
) implements Comparable<Row>{

    @Override
    public int compareTo(Row o) {
        return key.compareTo(o.key());
    }
}
