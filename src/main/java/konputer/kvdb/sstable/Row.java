package konputer.kvdb.sstable;

import konputer.kvdb.ValueHolder;

public record Row(
        String key,
        ValueHolder value
) {
}
