package konputer.kvdb.sstable;

public interface SSTableAction {
    void execute(SSTableHandle handle) throws Exception;
}
