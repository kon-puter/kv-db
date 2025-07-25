package konputer.kvdb.sstable;

import java.io.DataOutputStream;
import java.io.IOException;

public record SSTableHeader(
        int table_id,
        long size
) {
    public void serialize(DataOutputStream os) throws IOException {
        os.writeInt(table_id);
        os.writeLong(size);
    }
}
