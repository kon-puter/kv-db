package konputer.kvdb.persistent;

import konputer.kvdb.utils.RowTransformingIterable;
import konputer.kvdb.compaction.Compactable;
import konputer.kvdb.dtos.Row;
import org.jooq.lambda.Seq;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

public class SSTableMerger {

    public static SSTableHandle merge(List<? extends Compactable> compactables, SSTableContentBuilder builder) throws IOException {
        Iterable<Row> iterable = new RowTransformingIterable(
                Seq.seq(compactables)
                        .flatMap(c -> c.getBlocks().stream())
                        .toList()
        );

        Iterator<Row> it = iterable.iterator();


        Row lastRow = null;
        for (Row cur : iterable) {
            if (lastRow != null && lastRow.key().equals(cur.key())) {
                continue;
            }
            builder.writeKv(cur.key(), cur.value());
            lastRow = cur;
        }

        return builder.build();
    }

}
