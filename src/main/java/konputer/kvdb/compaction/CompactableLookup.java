package konputer.kvdb.compaction;

import konputer.kvdb.Lookup;
import konputer.kvdb.dtos.TaggedKey;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;

public interface CompactableLookup extends Compactable, Lookup, AutoCloseable {
    List<Iterator<ByteBuffer>> getRawBlocks(TaggedKey from, TaggedKey to);
}
