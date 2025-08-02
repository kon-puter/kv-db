package konputer.kvdb;

public class DbView  implements KvStore{
    @Override
    public void set(String key, byte[] value) {

    }

    @Override
    public ValueHolder get(String key) {
        return null;
    }

    @Override
    public boolean containsKey(String key) {
        return false;
    }

    @Override
    public boolean cas(String key, byte[] newVal, byte[] expected) {
        return false;
    }

    @Override
    public void remove(String key) {

    }

}
