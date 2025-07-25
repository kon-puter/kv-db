package konputer.kvdb;

import java.io.DataOutputStream;
import java.io.IOException;

public record ValueHolder(
        byte[] value
) {
    @Override
    public boolean equals(Object o) {
        return o instanceof ValueHolder &&
                java.util.Arrays.equals(value, ((ValueHolder)o).value);
    }
}
