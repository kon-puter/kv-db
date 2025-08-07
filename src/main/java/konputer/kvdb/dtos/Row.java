package konputer.kvdb.dtos;

public record Row(
        TaggedKey key,
        ValueHolder value
) implements Comparable<Row> {

    @Override
    public int compareTo(Row o) {
        return key.compareTo(o.key());
    }
}
