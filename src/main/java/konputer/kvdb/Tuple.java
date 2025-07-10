package konputer.kvdb;

public class Tuple {
    // transaction id, used for multi-version concurrency control mechanism
    // TODO: prevent transaction id wraparound
    int txid;
    Object data;
    ValType type;

    public Tuple(int txid, Object data, ValType type) {
        this.txid = txid;
        this.data = data;
        this.type = type;
    }


}
