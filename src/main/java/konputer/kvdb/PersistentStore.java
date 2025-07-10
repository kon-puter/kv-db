package konputer.kvdb;

import konputer.kvdb.sstable.SSTableHandle;

import java.util.ArrayList;
import java.util.Optional;

public class PersistentStore implements AutoCloseable, Lookup {

    public static final int GROW_FACTOR = 10;

    private int maxTblId = 0;

    // uses leveling approach

    private final LayerManager l0;
    private final ArrayList<SSTableHandle> layers;

    public PersistentStore() {
        this.l0 = new LayerManager();
        this.layers = new ArrayList<>();
    }

    public void addSSTable(SSTableHandle sstable) {
        if (sstable == null) {
            throw new IllegalArgumentException("SSTableHandle cannot be null");
        }
        l0.addSSTable(sstable);
        maxTblId = Math.max(maxTblId, sstable.header().table_id());
    }

//    void newSSTable(SSTableAction createAction) {
//        maxTblId++;
//        File tblFile = new File("table_" + maxTblId + ".sst");
//        try (
//                SSTableHandle handle = new
//        ) {
//            createAction.execute(handle);
//        } catch (Exception e) {
//            //TODO: handle exception properly
//            throw new RuntimeException("Failed to create SSTable file: " + tblFile.getAbsolutePath(), e);
//        }
//    }


    @Override
    public ValueHolder get(String key) throws Exception {
        ValueHolder vh = l0.get(key);
        if (vh != null) {
            return vh;
        }
        for (SSTableHandle handle : layers) {
            vh = handle.get(key);
            if (vh != null) {
                return vh;
            }
        }
        return null;
    }

    @Override
    public void close() {
        for (SSTableHandle handle : l0.sstables) {
            try {
                handle.close();
            } catch (Exception e) {
                //TODO: handle exception properly
                throw new RuntimeException("Failed to close SSTable file: " + handle.file().getAbsolutePath(), e);
            }
        }
        for (SSTableHandle handle : layers) {
            try {
                handle.close();
            } catch (Exception e) {
                //TODO: handle exception properly
                throw new RuntimeException("Failed to close SSTable file: " + handle.file().getAbsolutePath(), e);
            }
        }
    }
}
