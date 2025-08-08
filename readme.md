# KV-DB — LSM-Based Key-Value Store (Java)

KV-DB is a simple key-value database implemented in Java for learning
purposes.  It uses an LSM-tree storage engine with leveling compaction
and Bloom filters to explore techniques used in modern write-optimized
databases. 

While it’s primarily a learning project, it’s reasonably performant
and can handle moderate workloads. Although it is not tested enough and
is nowhere near production ready.

## Features
	
-    LSM-tree storage with SSTables
-	Multithreaded access
-	Snapshot isolation
-    Leveling compaction to reduce read amplification
-    Bloom filters for faster lookups
-    Memtable flush to SSTables

## Architecture

-    Memtable: In-memory sorted map
-    SSTables: Immutable, sorted disk files
-    Compaction: Merge SSTables into higher levels
-    Bloom filters: Quickly skip keys that are not present


## Why LSM?

LSM-trees are designed for:

-    Efficient sequential writes
-    Reducing random disk I/O
-    Handling large datasets with predictable performance

## Roadmap

- Compression
- redo log (WAL)
