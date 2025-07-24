package konputer.kvdb;

import com.google.common.math.LongMath;
import konputer.kvdb.sstable.*;
import org.jooq.lambda.Seq;

import java.io.File;
import java.io.IOException;
import java.lang.invoke.VarHandle;
import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class LevelingCompaction implements CompactionStrategy {

    private final PersistentStore store;
    private static final int GROW_FACTOR = 10;
    public static final LayerManager EMPTY_LAYER = new LayerManager();

    public LevelingCompaction(PersistentStore store) {
        this.store = store;
    }


    @Override
    public void ensureCompacted() {
        List<CompactableLookup> comp = store.getCompactables();

        List<Long> prefixSumMaxSize = Seq.range(1, comp.size() + 1)
                .map(i -> MemStore.MAX_MEMTABLE_SIZE * LongMath.pow(10, i))
                .scanLeft(0L, Long::sum).skip(1).toUnmodifiableList();

        List<Long> prefixSumCompSizes = Seq.seq(comp).map(Compactable::getSize).scanLeft(0L, Long::sum)
                .skip(1).toUnmodifiableList();

        checkState(prefixSumMaxSize.size() == prefixSumCompSizes.size(), "Internal error: prefix sums size mismatch");

        Optional<Integer> compactTo = Optional.empty();

        for (int i = comp.size() - 1; i >= 0; i--) {
            if (prefixSumCompSizes.get(i) > prefixSumMaxSize.get(i)) {
                compactTo = Optional.of(i);
                break;
            }
        }

        if (compactTo.isEmpty()) {
            return; // nothing to compact
        }

        doCompaction(compactTo.get(), comp, prefixSumCompSizes);

    }

    private void doCompaction(int compactTo, List<CompactableLookup> comp, List<Long> prefixSumCompSizes) {
        checkState(compactTo < comp.size(), "Compact to index is out of bounds");

        int tblId = store.currentTblId.addAndGet(1);
        try (
                SSTableContentBuilder builder = new SSTableContentBuilder(new File("tbl_" + tblId + ".sstable"),
                        new SSTableHeader(tblId, 0, prefixSumCompSizes.get(prefixSumCompSizes.size() - 1)))
        ) {
            if (compactTo == comp.size() - 1) {
                SSTableHandle h = SSTableMerger.merge(comp.subList(0, compactTo + 1), builder);
                comp.add(h);

            } else {
                // toElement is exclusive, so we need to add 2 to include the next layer
                List<CompactableLookup> toCompact = comp.subList(0, compactTo + 2);
                SSTableHandle h = SSTableMerger.merge(toCompact, builder);


                //newly compacted layer is switched
                comp.set(compactTo + 1, h);
            }
            VarHandle.fullFence();

            comp.get(0).supersededNotification();

            // remove leveled layers that were compacted
            // layer 0 must not be removed
            synchronized (store) {
                for (int i = 1; i <= compactTo; i++) {
                    comp.get(i).supersededNotification();
                    comp.set(i, EMPTY_LAYER);
                }
            }


        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
