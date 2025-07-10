package konputer.kvdb;

public class Main {

    public static void main(String[] args) throws Exception {
        Db db = new Db();
        for(int i = 0; i < 10000; i++) {
            db.set("key" + i, ("value" + i).getBytes());
        }
        db.flush();
        Thread.currentThread().sleep(1000); // Ensure flush completes
        long start = System.nanoTime();
        for(int i = 0; i < 10000; i++) {
            ValueHolder value = db.get("key" + i);
            if (value == null) {
                System.out.println("Key not found: key" + i);
            } else {
                String strValue = new String(value.value());
                if (!strValue.equals("value" + i)) {
                    System.out.println("Value mismatch for key" + i + ": expected value" + i + ", got " + strValue);
                }
            }
        }
        long end = System.nanoTime();
        System.out.println("Time taken to read 10000 keys: " + (end - start) / 1_000_000 + " ms");
        db.close();
        System.out.println("done");
    }
}
