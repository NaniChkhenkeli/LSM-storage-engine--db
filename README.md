# LSM Tree Database

A high-performance, disk-based key-value storage engine implementing the Log-Structured Merge-Tree (LSM) architecture.

## Features

- **Write-Optimized Storage**: Efficient handling of high write throughput
- **Tiered Storage**: Multi-level SSTable organization with compaction
- **Crash Recovery**: Write-Ahead Log (WAL) for durability
- **Efficient Reads**: Bloom filters and sparse/dense indexing
- **Range Queries**: Support for scanning key ranges
- **Statistics Tracking**: Performance metrics collection
- **Thread-Safe Operations**: Concurrent read/write support


## Components

### 1. MemTable
- In-memory sorted structure (TreeMap)
- Flushed to disk when size/memory thresholds are reached
- Serves recent writes for fast reads

### 2. Write-Ahead Log (WAL)
- Persistent log of all operations
- Ensures durability and crash recovery
- Cleared after successful MemTable flush

### 3. SSTables (Sorted String Tables)
- Immutable on-disk sorted key-value stores
- Organized in levels (0 = newest, higher = older/compacted)
- Each contains:
  - Data file (sorted entries)
  - Index file (sparse/dense)
  - Bloom filter (for quick existence checks)
  - Metadata (key range, count, etc.)

### 4. Compaction Manager
- Merges and reorganizes SSTables
- Reduces storage overhead and improves read performance
- Implements tiered compaction strategy


### Configuration
Key parameters (set as constants in LSMDatabase):

MEMTABLE_SIZE_THRESHOLD: Entry count trigger for flushing (default: 8)

MEMTABLE_MEMORY_THRESHOLD: Memory usage trigger for flushing (default: 5KB)

DENSE_INDEX_THRESHOLD: SSTable size for using dense vs. sparse index (default: 10,000)

SPARSE_INDEX_INTERVAL: Key interval for sparse index (default: 100)
