package konputer.kvdb.sstable;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public interface Compactable {

    long getSize();

    List<Iterator<ByteBuffer>> getBlocks();

    void supersededNotification();


}
