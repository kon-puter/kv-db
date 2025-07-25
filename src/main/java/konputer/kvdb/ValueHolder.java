package konputer.kvdb;

import java.io.DataOutputStream;
import java.io.IOException;

public record ValueHolder(
        byte[] value
) {
    public void serialize(DataOutputStream os) throws IOException {
        os.writeInt(value.length);
        os.write(value);
    }
}
