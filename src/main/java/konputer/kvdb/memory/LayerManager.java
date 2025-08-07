package konputer.kvdb.memory;

import konputer.kvdb.Lookup;
import konputer.kvdb.dtos.TaggedKey;
import konputer.kvdb.dtos.ValueHolder;
import konputer.kvdb.compaction.Compactable;
import konputer.kvdb.compaction.CompactableLookup;
import konputer.kvdb.persistent.SSTableHandle;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.LongSummaryStatistics;

public class LayerManager implements CompactableLookup, Lookup, Compactable, AutoCloseable {
    private final ArrayList<SSTableHandle> sstables;
    private final LongSummaryStatistics sizeStats;

    public LayerManager() {
        this.sstables = new ArrayList<>();
        this.sizeStats = new LongSummaryStatistics();
    }

    public void addSSTable(SSTableHandle sstable) {
        this.sstables.add(sstable);
        this.sizeStats.accept(sstable.getSize());
    }

    public LongSummaryStatistics getSizeStats() {
        return sizeStats;
    }

    @Override
    public long getSize() {
        return getSizeStats().getSum();
    }

    @Override
    public synchronized ValueHolder get(String key) throws Exception {
        for (int i = sstables.size() - 1; i >= 0; i--) {
            ValueHolder value = sstables.get(i).get(key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    @Override
    public List<Iterator<ByteBuffer>> getBlocks() {
        return sstables.stream()
                .flatMap(e -> e.getBlocks().stream())
                .toList();
    }

    @Override
    public synchronized void supersededNotification() {
        RuntimeException e = null;
        for (SSTableHandle sstable : sstables) {
            try {
                sstable.supersededNotification();
            } catch (RuntimeException t) {
                if (e == null) {
                    e = new RuntimeException("error in removal", t);
                } else {
                    e.addSuppressed(t);
                }
            }
        }

        sstables.clear();
        sizeStats.accept(-sizeStats.getSum());
    }

    @Override
    public List<Iterator<ByteBuffer>> getRawBlocks(TaggedKey from, TaggedKey to) {
        return (
                sstables.stream()
                        .map(sstable -> sstable.blockRangeIterator(from, to))
                        .toList()
        );
    }


    @Override
    public void close() {
        Throwable toThrow = null;
        for (SSTableHandle sstable : sstables) {
            try {
                sstable.close();
            } catch (Throwable e) {
                if (toThrow == null) {
                    toThrow = e;
                } else {
                    toThrow.addSuppressed(e);
                }
            }
        }
    }
}
