package konputer.kvdb;

import com.google.common.collect.Iterators;
import konputer.kvdb.sstable.Row;
import konputer.kvdb.sstable.SSTableHandle;
import konputer.kvdb.sstable.SSTableMerger;
import org.jooq.lambda.Seq;
import org.jspecify.annotations.NonNull;

import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class RowTransformingIterable implements Iterable<Row> {

    private final List<Iterator<ByteBuffer>> source;
    public RowTransformingIterable(List<Iterator<ByteBuffer>> source) {
        this.source = source;
    }

    @Override
    public Iterator<Row> iterator() {

        //Order is from newest to oldest, needed for correct deduplication
        Iterator<Iterator<Row>> toMerge = Seq.seq(source)
                .map(ib ->
                        Seq.seq(ib)
                                .map(b -> new SSTableHandle.RowAwareBlock(b).rowIterator())
                                .flatMap(Seq::seq)
                                .iterator()
                ).iterator();
        return Iterators.mergeSorted(
                new Iterable<Iterator<Row>>() {
                    @Override
                    @NonNull
                    public Iterator<Iterator<Row>> iterator() {
                        return toMerge;
                    }
                }
                , Comparator.naturalOrder());
    }

}
