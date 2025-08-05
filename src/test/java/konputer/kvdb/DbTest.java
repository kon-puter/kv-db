package konputer.kvdb;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.LongAdder;

import static org.junit.jupiter.api.Assertions.*;

class DbTest {
    private Db kvStore;

    @BeforeEach
    void setUp() {
        kvStore = new Db(); // Replace with other KvStore implementations as needed
    }

    @Test
    void testSetAndGet() {
        String key = "foo";
        byte[] value = "bar".getBytes();
        kvStore.set(key, value);
        ValueHolder holder = kvStore.get(key);
        assertNotNull(holder);
        assertArrayEquals(value, holder.value());
    }

    @Test
    void testContainsKey() {
        String key = "exists";
        kvStore.set(key, "val".getBytes());
        assertTrue(kvStore.containsKey(key));
        assertFalse(kvStore.containsKey("missing"));
    }

    @Test
    void testCas() {
        String key = "casKey";
        byte[] initial = "init".getBytes();
        byte[] expected = "init".getBytes();
        byte[] newVal = "new".getBytes();
        kvStore.set(key, initial);
        boolean success = kvStore.cas(key, newVal, expected);
        assertTrue(success);
        assertArrayEquals(newVal, kvStore.get(key).value());
        // Should fail if expected doesn't match
        assertFalse(kvStore.cas(key, initial, expected));
    }

    @Test
    void testRemove() {
        String key = "removeMe";
        kvStore.set(key, "bye".getBytes());
        kvStore.remove(key);
        assertFalse(kvStore.containsKey(key));
        assertNull(kvStore.getSimple(key));
    }

    @Test
    void testRawIterateFullRange() {
        // Insert a set of key-value pairs
        int count = 100;
        for (int i = 0; i < count; i++) {
            kvStore.set("key" + i, ("val" + i).getBytes());
        }
        // Use rawIterate to iterate over all keys (assuming TaggedKey is comparable by key string)
        TaggedKey from = new TaggedKey("key0", 0);
        TaggedKey to = new TaggedKey("key" + (count - 1), Long.MAX_VALUE);
        java.util.Map<String, String> seen = new java.util.HashMap<>();
        for (var rowIt = ((Db)kvStore).rawIterate(from, to); rowIt.hasNext(); ) {
            var row = rowIt.next();
            seen.put(row.key().key(), new String(row.value().value()));
        }
        for (int i = 0; i < count; i++) {
            String k = "key" + i;
            String v = "val" + i;
            assertEquals(v, seen.get(k));
        }
        assertEquals(count, seen.size());
    }

    @Test
    void testRawIterateEmptyRange() {
        TaggedKey from = new TaggedKey("zzz", 0);
        TaggedKey to = new TaggedKey("zzzz", 0);
        var it = ((Db)kvStore).rawIterate(from, to);
        assertFalse(it.hasNext());
    }

    @Test
    void testRawIterateWithRemovals() {
        kvStore.set("a", "1".getBytes());
        kvStore.set("b", "2".getBytes());
        kvStore.set("c", "3".getBytes());
        kvStore.remove("b");
        TaggedKey from = new TaggedKey("a", 0);
        TaggedKey to = new TaggedKey("c", Long.MAX_VALUE);
        java.util.Set<String> keys = new java.util.HashSet<>();
        for (var it = ((Db)kvStore).rawIterate(from, to); it.hasNext(); ) {
            var row = it.next();
            if (!row.value().isTombstone()) {
                keys.add(row.key().key());
            }
        }
        assertTrue(keys.contains("a"));
        assertFalse(keys.contains("b"));
        assertTrue(keys.contains("c"));
    }





    /**
     * Operation ratios for benchmarking KvStore.
     */
    public static class OperationRatios {
        public final double read;
        public final double write;
        public final double cas;
        public final double remove;
        public OperationRatios(double read, double write, double cas, double remove) {
            double sum = read + write + cas + remove;
            if (Math.abs(sum - 1.0) > 1e-6) throw new IllegalArgumentException("Ratios must sum to 1.0");
            this.read = read;
            this.write = write;
            this.cas = cas;
            this.remove = remove;
        }
        @Override
        public String toString() {
            return String.format("read=%.2f, write=%.2f, cas=%.2f, remove=%.2f", read, write, cas, remove);
        }
    }

    /**
     * Runs a multithreaded benchmark on the KvStore with configurable operation ratios.
     * @param threads Number of threads
     * @param opsPerThread Operations per thread
     * @param ratios OperationRatios instance
     * @param keyCount Number of keys to use
     * @return Stats for each operation and elapsed time in ms
     */
    private BenchmarkResult runKvStoreBenchmark(int threads, int opsPerThread, OperationRatios ratios, int keyCount) throws InterruptedException {
        String[] keys = new String[keyCount];
        for (int i = 0; i < keys.length; i++) keys[i] = "k" + i;
        Random rand = new Random();
        CountDownLatch latch = new CountDownLatch(threads);
        LongAdder reads = new LongAdder();
        LongAdder writes = new LongAdder();
        LongAdder cass = new LongAdder();
        LongAdder removes = new LongAdder();
        long start = System.nanoTime();
        for (int t = 0; t < threads; t++) {
            new Thread(() -> {
                for (int i = 0; i < opsPerThread; i++) {
                    double op = rand.nextDouble();
                    String key = keys[rand.nextInt(keys.length)];
                    if (op < ratios.read) {
                        if (kvStore.get(key) != null)
                            reads.increment();
                    } else if (op < ratios.read + ratios.write) {
                        kvStore.set(key, ("v" + rand.nextInt()).getBytes());
                        writes.increment();
                    } else if (op < ratios.read + ratios.write + ratios.cas) {
                        ValueHolder v = kvStore.get(key);
                        if (v != null) {
                            boolean ok = kvStore.cas(key, ("c" + rand.nextInt()).getBytes(), v.value());
                            if (ok) cass.increment();
                        }
                    } else {
                        if (kvStore.containsKey(key)) {
                            kvStore.remove(key);
                            removes.increment();
                        }
                    }
                   // kvStore.size();
                }
                latch.countDown();
            }).start();
        }
        latch.await();
        long elapsed = System.nanoTime() - start;
        return new BenchmarkResult(reads.sum(), writes.sum(), cass.sum(), removes.sum(),  elapsed/1000000);
    }

    static class BenchmarkResult {
        final long reads, writes, cass, removes;
        final long  elapsedMs;
        BenchmarkResult(long reads, long writes, long cass, long removes,  long elapsedMs) {
            this.reads = reads;
            this.writes = writes;
            this.cass = cass;
            this.removes = removes;
            this.elapsedMs = elapsedMs;
        }
        @Override
        public String toString() {
            return String.format("Reads: %d, Writes: %d, CAS: %d, Removes: %d, Time: %dms",
                    reads, writes, cass, removes,  elapsedMs);
        }
    }

    @Test
    void testBenchmarkVariousRatios() throws InterruptedException {
        List<OperationRatios> ratioSets = List.of(
                new OperationRatios(0.7, 0.1, 0.1, 0.1),
                new OperationRatios(0.5, 0.3, 0.15, 0.05),
                new OperationRatios(0.25, 0.25, 0.25, 0.25),
                new OperationRatios(0.9, 0.05, 0.03, 0.02)
        );
        for (OperationRatios ratios : ratioSets) {
            kvStore = new Db(); // Reset for each run
            BenchmarkResult result = runKvStoreBenchmark(32, 100000, ratios, 100);
            System.out.println("Ratios: " + ratios + " -> " + result);
            assertTrue(result.reads > 0);
            assertTrue(result.writes > 0);
        }
    }
}
