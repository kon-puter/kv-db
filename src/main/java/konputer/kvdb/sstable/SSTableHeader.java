package konputer.kvdb.sstable;

import java.io.DataOutputStream;
import java.io.IOException;

public record SSTableHeader(
        int table_id,
        int maxTxid,
        long size
) {
    public void serialize(DataOutputStream os) throws IOException {
        os.writeInt(table_id);
        os.writeInt(maxTxid);
        os.writeLong(size);
    }
}
