package konputer.kvdb;

import java.io.DataOutputStream;
import java.io.IOException;

public record ValueHolder(
        int txid,
        byte[] value
) {
    public void serialize(DataOutputStream os) throws IOException {
        os.writeInt(txid);
        os.writeInt(value.length);
        os.write(value);
    }
}
