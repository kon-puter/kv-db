package konputer.kvdb.sstable;

public record Row(
        String key,
        byte[] value
) {
}
