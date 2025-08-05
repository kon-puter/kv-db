package konputer.kvdb;

import konputer.kvdb.sstable.CompactableLookup;
import konputer.kvdb.sstable.Row;
import konputer.kvdb.sstable.SSTableHandle;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

public class PersistentStore implements AutoCloseable, Lookup {


    final AtomicInteger currentTblId = new AtomicInteger(0);


    // uses leveling approach

    private final LayerManager l0;
    private final ArrayList<CompactableLookup> layers;

    private final CompactionStrategy compactionStrategy = new LevelingCompaction(this);

    public PersistentStore() {
        this.l0 = new LayerManager();
        this.layers = new ArrayList<>();
        this.layers.add(l0);
    }

    public void addSSTable(SSTableHandle sstable) {
        if (sstable == null) {
            throw new IllegalArgumentException("SSTableHandle cannot be null");
        }
        l0.addSSTable(sstable);
        compactionStrategy.ensureCompacted();
    }

    List<CompactableLookup> getCompactables() {
        return layers;
    }

    public Iterator<Row> getRawRange(TaggedKey from, TaggedKey to) {
        RowTransformingIterable transformer = new RowTransformingIterable(
                layers.stream().flatMap(l -> l.getRawBlocks(from, to).stream()).toList()
        );
        return transformer.iterator();

    }

    @Override
    public synchronized ValueHolder get(String key) throws Exception {
        for (Lookup handle : layers) {
            var vh = handle.get(key);
            if (vh != null) {
                return vh;
            }
        }
        return null;
    }

    @Override
    public void close() {
        for (AutoCloseable handle : layers) {
            try {
                handle.close();
            } catch (Throwable e) {
                throw new RuntimeException("Error closing handle", e);
            }
        }
    }


}
