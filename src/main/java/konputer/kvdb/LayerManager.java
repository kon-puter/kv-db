package konputer.kvdb;

import konputer.kvdb.sstable.Compactable;
import konputer.kvdb.sstable.CompactableLookup;
import konputer.kvdb.sstable.SSTableHandle;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.stream.Collectors;

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
    public ValueHolder get(String key) throws Exception {
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
    public void supersededNotification() {
        RuntimeException e = null;
        for(SSTableHandle sstable : sstables) {
            try {
                sstable.supersededNotification();
            }catch (RuntimeException t){
                if(e == null){
                    e = new RuntimeException("error in removal", t);
                }
                else {
                    e.addSuppressed(t);
                }
            }
        }

        sstables.clear();
        sizeStats.accept(-sizeStats.getSum());
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
