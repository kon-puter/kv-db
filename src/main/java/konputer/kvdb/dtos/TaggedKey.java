package konputer.kvdb.dtos;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public record TaggedKey(
        String key,
        long snapshotId
) implements Comparable<TaggedKey> {


    public long serialize(DataOutputStream os) throws IOException {
        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        os.writeInt(keyBytes.length);
        os.write(keyBytes);
        os.writeLong(snapshotId);
        return keyBytes.length + Long.BYTES + Integer.BYTES; // +4 for the length of the key
    }

    public static TaggedKey deserialize(ByteBuffer buf) {
        int keyLength = buf.getInt();
        byte[] keyBytes = new byte[keyLength];
        buf.get(keyBytes);
        long snapshotId = buf.getLong();
        return new TaggedKey(new String(keyBytes, StandardCharsets.UTF_8), snapshotId);
    }

    @Override
    public int compareTo(TaggedKey o) {
        int keyComparison = this.key.compareTo(o.key);
        if (keyComparison != 0) {
            return keyComparison;
        }
        // snapshotId comparison so newer entries come first
        return Long.compare(this.snapshotId, o.snapshotId);
    }
}
