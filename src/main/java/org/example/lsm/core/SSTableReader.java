package org.example.lsm.core;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Enhanced SSTableReader with range query support and improved performance
 * - Supports efficient range queries and key lookups
 * - Thread-safe operations with proper locking
 * - Bloom filter integration for optimized reads
 * - MVCC support with timestamp and generation tracking
 * - Comprehensive error handling and recovery
 */
public class SSTableReader implements AutoCloseable {
    private final String dataDir;
    private final int generation;
    private final Path dataPath;
    private final Path indexPath;
    private final Path bloomPath;
    private final Path metaPath;

    private BloomFilter bloomFilter;
    private Map<String, Long> sparseIndex;
    private Map<String, Long> denseIndex; // Full index for smaller SSTables
    public SSTableMetadata metadata;

    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private volatile boolean closed = false;
    private final boolean useDenseIndex;

    // Statistics
    private volatile long totalReads = 0;
    private volatile long bloomFilterHits = 0;
    private volatile long bloomFilterMisses = 0;
    private volatile long indexHits = 0;
    private volatile long diskReads = 0;

    private static final int DENSE_INDEX_THRESHOLD = 10000; // Use dense index for small SSTables
    private static final int SPARSE_INDEX_INTERVAL = 100; // Every 100th key in sparse index

    public SSTableReader(String dataDir, int generation) throws IOException {
        this.dataDir = dataDir;
        this.generation = generation;
        this.dataPath = Paths.get(dataDir, String.format("sstable_%06d.data", generation));
        this.indexPath = Paths.get(dataDir, String.format("sstable_%06d.index", generation));
        this.bloomPath = Paths.get(dataDir, String.format("sstable_%06d.bloom", generation));
        this.metaPath = Paths.get(dataDir, String.format("sstable_%06d.meta", generation));

        loadMetadata();
        this.useDenseIndex = metadata.getEntryCount() <= DENSE_INDEX_THRESHOLD;
        loadIndex();
        loadBloomFilter();

        System.out.println("📖 Loaded SSTable gen-" + generation +
                " (level=" + metadata.getLevel() +
                ", entries=" + metadata.getEntryCount() +
                ", index=" + (useDenseIndex ? "dense" : "sparse") + ")");
    }

    private void loadMetadata() throws IOException {
        if (!Files.exists(metaPath)) {
            throw new FileNotFoundException("SSTable metadata file not found: " + metaPath);
        }
        this.metadata = SSTableMetadata.loadFromFile(metaPath);
    }

    private void loadIndex() throws IOException {
        if (useDenseIndex) {
            loadDenseIndex();
        } else {
            loadSparseIndex();
        }
    }

    private void loadDenseIndex() throws IOException {
        this.denseIndex = new HashMap<>();
        if (!Files.exists(indexPath)) {
            throw new FileNotFoundException("SSTable index file not found: " + indexPath);
        }

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(indexPath)))) {
            while (in.available() > 0) {
                int keyLen = in.readInt();
                byte[] keyBytes = new byte[keyLen];
                in.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);
                long offset = in.readLong();
                denseIndex.put(key, offset);
            }
        }
    }

    private void loadSparseIndex() throws IOException {
        this.sparseIndex = new TreeMap<>();
        if (!Files.exists(indexPath)) {
            throw new FileNotFoundException("SSTable index file not found: " + indexPath);
        }

        try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(indexPath)))) {
            int count = 0;
            while (in.available() > 0) {
                int keyLen = in.readInt();
                byte[] keyBytes = new byte[keyLen];
                in.readFully(keyBytes);
                String key = new String(keyBytes, StandardCharsets.UTF_8);
                long offset = in.readLong();

                // Only keep every Nth key in sparse index
                if (count % SPARSE_INDEX_INTERVAL == 0) {
                    sparseIndex.put(key, offset);
                }
                count++;
            }
        }
    }

    private void loadBloomFilter() throws IOException {
        if (!Files.exists(bloomPath)) {
            System.err.println("⚠️  Bloom filter file not found for gen " + generation +
                    ". Creating empty filter - performance will be impacted.");
            this.bloomFilter = new BloomFilter(1, 0.99);
            return;
        }

        try {
            this.bloomFilter = BloomFilter.readFromFile(bloomPath.toString());
        } catch (Exception e) {
            System.err.println("⚠️  Failed to load Bloom filter for gen " + generation +
                    ": " + e.getMessage() + ". Creating empty filter.");
            this.bloomFilter = new BloomFilter(1, 0.99);
        }
    }

    // Getters
    public int getGeneration() { return generation; }
    public int getLevel() { return metadata != null ? metadata.getLevel() : 0; }
    public String getMinKey() { return metadata != null ? metadata.getMinKey() : null; }
    public String getMaxKey() { return metadata != null ? metadata.getMaxKey() : null; }
    public SSTableMetadata getMetadata() { return metadata; }

    /**
     * Check if the key might exist using Bloom filter
     */
    public boolean mightContain(String key) {
        if (closed) return false;

        lock.readLock().lock();
        try {
            totalReads++;
            boolean result = bloomFilter.mightContain(key);
            if (result) {
                bloomFilterHits++;
            } else {
                bloomFilterMisses++;
            }
            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get value for a specific key
     */
    public KeyValuePair get(String key) throws IOException {
        if (closed) return null;

        lock.readLock().lock();
        try {
            // First check Bloom filter
            if (!mightContain(key)) {
                return null;
            }

            // Check if key is in range
            if (!metadata.mightContainKey(key)) {
                return null;
            }

            Long offset = findKeyOffset(key);
            if (offset == null) {
                return null;
            }

            indexHits++;
            return readEntryAtOffset(offset, key);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Find the offset for a key using either dense or sparse index
     */
    private Long findKeyOffset(String key) throws IOException {
        if (useDenseIndex) {
            return denseIndex.get(key);
        } else {
            return findKeyOffsetInSparseIndex(key);
        }
    }

    /**
     * Find key offset using sparse index and sequential search
     */
    private Long findKeyOffsetInSparseIndex(String key) throws IOException {
        // Find the largest key in sparse index that is <= target key
        String floorKey = null;
        Long floorOffset = null;

        for (Map.Entry<String, Long> entry : sparseIndex.entrySet()) {
            if (entry.getKey().compareTo(key) <= 0) {
                floorKey = entry.getKey();
                floorOffset = entry.getValue();
            } else {
                break;
            }
        }

        if (floorOffset == null) {
            return null; // Key would be before the first indexed key
        }

        // Sequential search from floor position
        return sequentialSearchFromOffset(key, floorOffset);
    }

    /**
     * Sequential search for key starting from a given offset
     */
    private Long sequentialSearchFromOffset(String targetKey, long startOffset) throws IOException {
        diskReads++;
        try (RandomAccessFile raf = new RandomAccessFile(dataPath.toFile(), "r")) {
            raf.seek(startOffset);

            while (raf.getFilePointer() < raf.length()) {
                long currentOffset = raf.getFilePointer();

                int keyLen = raf.readInt();
                byte[] keyBytes = new byte[keyLen];
                raf.readFully(keyBytes);
                String currentKey = new String(keyBytes, StandardCharsets.UTF_8);

                if (currentKey.equals(targetKey)) {
                    return currentOffset;
                } else if (currentKey.compareTo(targetKey) > 0) {
                    return null; // Passed the target key
                }

                // Skip the rest of this entry
                int valueLen = raf.readInt();
                raf.skipBytes(valueLen + 1 + 8 + 4); // value + isDeleted + timestamp + generation
            }
        }
        return null;
    }

    /**
     * Read entry at specific offset
     */
    private KeyValuePair readEntryAtOffset(long offset, String expectedKey) throws IOException {
        diskReads++;
        try (RandomAccessFile raf = new RandomAccessFile(dataPath.toFile(), "r")) {
            raf.seek(offset);

            int keyLen = raf.readInt();
            byte[] keyBytes = new byte[keyLen];
            raf.readFully(keyBytes);
            String foundKey = new String(keyBytes, StandardCharsets.UTF_8);

            if (!foundKey.equals(expectedKey)) {
                System.err.println("⚠️  Key mismatch at offset " + offset +
                        ": expected '" + expectedKey + "', found '" + foundKey + "'");
                return null;
            }

            int valueLen = raf.readInt();
            byte[] valueBytes = new byte[valueLen];
            raf.readFully(valueBytes);
            String value = valueLen > 0 ? new String(valueBytes, StandardCharsets.UTF_8) : null;

            byte isDeleted = raf.readByte();
            long timestamp = raf.readLong();
            int generation = raf.readInt();

            return new KeyValuePair(foundKey, value, isDeleted == 1, timestamp, generation);
        }
    }

    /**
     * Scan all entries in the SSTable
     */
    public Map<String, KeyValuePair> scanAllKeyValuePairs() throws IOException {
        if (closed) return Collections.emptyMap();

        lock.readLock().lock();
        try {
            Map<String, KeyValuePair> data = new TreeMap<>();
            if (!Files.exists(dataPath)) {
                System.err.println("⚠️  SSTable data file not found: " + dataPath);
                return data;
            }

            diskReads++;
            try (DataInputStream in = new DataInputStream(new BufferedInputStream(Files.newInputStream(dataPath)))) {
                while (in.available() > 0) {
                    int keyLen = in.readInt();
                    byte[] keyBytes = new byte[keyLen];
                    in.readFully(keyBytes);
                    String key = new String(keyBytes, StandardCharsets.UTF_8);

                    int valueLen = in.readInt();
                    byte[] valueBytes = new byte[valueLen];
                    in.readFully(valueBytes);
                    String value = valueLen > 0 ? new String(valueBytes, StandardCharsets.UTF_8) : null;

                    byte isDeleted = in.readByte();
                    long timestamp = in.readLong();
                    int generation = in.readInt();

                    KeyValuePair kvp = new KeyValuePair(key, value, isDeleted == 1, timestamp, generation);
                    data.put(key, kvp);
                }
            }
            return data;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Range scan with start and end keys (inclusive)
     */
    public Map<String, KeyValuePair> scanRange(String startKey, String endKey) throws IOException {
        if (closed) return Collections.emptyMap();

        lock.readLock().lock();
        try {
            // Check if range overlaps with this SSTable
            if (!metadata.overlapsRange(startKey, endKey)) {
                return Collections.emptyMap();
            }

            Map<String, KeyValuePair> result = new TreeMap<>();
            Map<String, KeyValuePair> allData = scanAllKeyValuePairs();

            for (Map.Entry<String, KeyValuePair> entry : allData.entrySet()) {
                String key = entry.getKey();
                if (key.compareTo(startKey) >= 0 && key.compareTo(endKey) <= 0) {
                    result.put(key, entry.getValue());
                }
            }

            return result;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Get statistics for this SSTable reader
     */
    public SSTableStats getStats() {
        lock.readLock().lock();
        try {
            return new SSTableStats(
                    generation, metadata.getLevel(), metadata.getEntryCount(),
                    metadata.getFileSize(), totalReads, bloomFilterHits, bloomFilterMisses,
                    indexHits, diskReads, useDenseIndex ? "dense" : "sparse"
            );
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Verify SSTable integrity
     */
    public boolean verify() throws IOException {
        if (closed) return false;

        lock.readLock().lock();
        try {
            // Check if all files exist
            if (!Files.exists(dataPath) || !Files.exists(indexPath) ||
                    !Files.exists(metaPath) || !Files.exists(bloomPath)) {
                return false;
            }

            // Verify data file size matches metadata
            long actualSize = Files.size(dataPath);
            if (actualSize != metadata.getFileSize()) {
                System.err.println("⚠️  File size mismatch for gen " + generation +
                        ": expected " + metadata.getFileSize() + ", actual " + actualSize);
                return false;
            }

            // TODO: Add checksum verification if available
            return true;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (!closed) {
                closed = true;
                // Clear caches to free memory
                if (denseIndex != null) {
                    denseIndex.clear();
                }
                if (sparseIndex != null) {
                    sparseIndex.clear();
                }
                System.out.println("🔒 Closed SSTable gen-" + generation);
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Statistics class for SSTable reader
     */
    public static class SSTableStats {
        public final int generation;
        public final int level;
        public final int entryCount;
        public final long fileSize;
        public final long totalReads;
        public final long bloomFilterHits;
        public final long bloomFilterMisses;
        public final long indexHits;
        public final long diskReads;
        public final String indexType;

        public SSTableStats(int generation, int level, int entryCount, long fileSize,
                            long totalReads, long bloomFilterHits, long bloomFilterMisses,
                            long indexHits, long diskReads, String indexType) {
            this.generation = generation;
            this.level = level;
            this.entryCount = entryCount;
            this.fileSize = fileSize;
            this.totalReads = totalReads;
            this.bloomFilterHits = bloomFilterHits;
            this.bloomFilterMisses = bloomFilterMisses;
            this.indexHits = indexHits;
            this.diskReads = diskReads;
            this.indexType = indexType;
        }

        public double getBloomFilterHitRate() {
            return totalReads > 0 ? (double) bloomFilterHits / totalReads : 0.0;
        }

        public double getIndexHitRate() {
            return totalReads > 0 ? (double) indexHits / totalReads : 0.0;
        }

        @Override
        public String toString() {
            return String.format("SSTableStats{gen=%d, level=%d, entries=%d, " +
                            "size=%.2fKB, reads=%d, bloom_hit_rate=%.2f%%, " +
                            "index_hits=%d, disk_reads=%d, index=%s}",
                    generation, level, entryCount, fileSize / 1024.0,
                    totalReads, getBloomFilterHitRate() * 100,
                    indexHits, diskReads, indexType);
        }
    }
}