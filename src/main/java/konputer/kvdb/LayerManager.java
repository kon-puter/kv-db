package konputer.kvdb;

import konputer.kvdb.sstable.SSTableHandle;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LongSummaryStatistics;
import java.util.Stack;

public class LayerManager implements Lookup{
    public final ArrayList<SSTableHandle> sstables;
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
    public ValueHolder get(String key) throws Exception {
        for(int i = sstables.size() - 1; i >= 0; i--) {
            ValueHolder value = sstables.get(i).get(key);
            if (value != null) {
                return value;
            }
        }
        return null;
    }
}
