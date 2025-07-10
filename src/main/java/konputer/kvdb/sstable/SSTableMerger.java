package konputer.kvdb.sstable;

import java.io.File;
import java.util.ArrayList;

public class SSTableMerger {

    public final ArrayList<SSTableHandle> toMerge;

    public SSTableMerger(ArrayList<SSTableHandle> toMerge) {
        this.toMerge = toMerge;
    }

    public SSTableMerger() {
        this.toMerge = new ArrayList<>();
    }


    public void addSSTable(SSTableHandle sstable) {
        this.toMerge.add(sstable);
    }

    public void mergeSSTables(File file, int tblId) throws Exception {

    }

}
