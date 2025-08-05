package konputer.kvdb;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public record ValueHolder(
        byte[] value, boolean deleted
) {
    private static final ValueHolder DELETED = new ValueHolder(null, true);
    public ValueHolder(byte[] value) {
        this(value, false);
    }

    public static ValueHolder tombstone() {
        return DELETED;
    }

    public long serialize(DataOutputStream os) throws IOException {
        if (value == null) {
            os.writeInt(-1); // indicates a tombstone
            return Integer.BYTES;
        } else {
            os.writeInt(value.length);
            os.write(value);
            return Integer.BYTES + value.length;
        }
    }

   //deserialize from ByteBuffer class not DataInputStream
    public static ValueHolder deserialize(ByteBuffer buf){
        int valueLength = buf.getInt();
        if(valueLength < 0) {
            return tombstone();
        }
        byte[] value = new byte[valueLength];
        buf.get(value);
        return new ValueHolder(value);
    }


    public boolean isTombstone() {
        return deleted;
    }

    public int length() {
        return (value == null ? 0 : value.length) + Integer.BYTES; // +4 for the length of the value
    }

    @Override
    public boolean equals(Object o) {
        return o instanceof ValueHolder &&
                java.util.Arrays.equals(value, ((ValueHolder)o).value);
    }
}
