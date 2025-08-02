package konputer.kvdb.sstable;

import com.google.common.collect.Iterators;
import org.jooq.lambda.Seq;

import java.io.IOException;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class SSTableMerger {

    private record RowWithOrder(Row row, long order) implements Comparable<RowWithOrder> {

        @Override
            public int compareTo(RowWithOrder o) {
                int cmp = row.key().compareTo(o.row.key());
                if (cmp != 0) {
                    return cmp;
                }
                return Long.compare(order, o.order);
            }
        }

    public static SSTableHandle merge(List<? extends Compactable> compactables, SSTableContentBuilder builder) throws IOException {
        //Order is from newest to oldest, needed for correct deduplication
        List<Iterator<RowWithOrder>> toMerge = Seq.seq(compactables)
                .flatMap(c -> c.getBlocks().stream()).zipWithIndex()
                .map(ib ->
                        Seq.seq(ib.v1)
                                .map(b -> new SSTableHandle.RowAwareBlock(b).rowIterator())
                                .flatMap(Seq::seq).map(r -> new RowWithOrder(r, ib.v2))
                                .iterator()
                )
                .toUnmodifiableList();
        Iterator<RowWithOrder> it = Iterators.mergeSorted(toMerge, Comparator.naturalOrder());

        RowWithOrder lastRow = null;
        while (it.hasNext()) {
            RowWithOrder cur = it.next();
            if(lastRow != null && lastRow.row.key().equals(cur.row.key())) {
                continue;
            }
            builder.writeKv(cur.row.key(), cur.row.value());
            lastRow = cur;
        }

        return builder.build();
    }

}
