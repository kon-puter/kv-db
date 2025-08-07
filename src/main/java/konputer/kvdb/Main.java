package konputer.kvdb;

import konputer.kvdb.dtos.ValueHolder;

public class Main {

    public static void main(String[] args) throws Exception {
        Db db = new Db();
        for (int i = 0; i < 1_000_000; i++) {
            db.set("key" + i, ("value" + i).getBytes());
        }
        Thread.sleep(10000); // Ensure flush completes
        long start = System.nanoTime();
        for (int i = 0; i < 1_000_000; i++) {
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
        System.out.println("Time taken to read 1000000 keys: " + (end - start) / 1_000_000.0 + " ms");
        db.close();
        System.out.println("done");
    }
}
