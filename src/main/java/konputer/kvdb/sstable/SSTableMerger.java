package konputer.kvdb.sstable;

import com.google.common.collect.Iterators;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple2;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class SSTableMerger {


    public static SSTableHandle merge(List<? extends Compactable> compactables, SSTableContentBuilder builder) throws IOException {
        //Order is from newest to oldest, needed for correct deduplication
        List<Iterator<Row>> toMerge = Seq.seq(compactables)
                .flatMap(c -> c.getBlocks().stream())
                .map(ib ->
                        Seq.seq(ib)
                                .map(b -> new SSTableHandle.RowAwareBlock(b).rowIterator())
                                .flatMap(Seq::seq)
                                .iterator()
                )
                .toUnmodifiableList();


        Iterator<Row> it = Iterators.mergeSorted(toMerge, Comparator.comparing(Row::key));

        while (it.hasNext()) {
            Row row = it.next();
            builder.writeKv(row.key(), row.value());
        }

        return builder.build();
    }

}
