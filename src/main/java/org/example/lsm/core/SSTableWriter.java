package org.example.lsm.core;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;
import java.util.TreeMap;

/**
 * SSTableWriter handles the creation of SSTables from MemTable data and compaction operations.
 * Features:
 * - Atomic writes using temporary files
 * - Bloom filter generation for efficient lookups
 * - Metadata tracking with key ranges
 * - Support for both regular flushes and compaction writes
 * - Proper error handling and cleanup
 */
public class SSTableWriter {
    private final String dataDir;

    public SSTableWriter(String dataDir) {
        this.dataDir = dataDir;
    }

    /**
     * Flush MemTable data to disk as a new SSTable at Level 0.
     */
    public SSTableReader flushToDisk(LSMDatabase.MemTable memTable, WAL wal, int generation) throws IOException {

        System.out.println("🚀 Starting MemTable flush to SSTable gen-" + generation);

        Path dbPath = Paths.get(dataDir, String.format("sstable_%06d.data", generation));
        Path indexPath = Paths.get(dataDir, String.format("sstable_%06d.index", generation));
        Path bloomPath = Paths.get(dataDir, String.format("sstable_%06d.bloom", generation));
        Path metaPath = Paths.get(dataDir, String.format("sstable_%06d.meta", generation));

        // Use temporary files for atomic writes
        Path tempDbPath = Paths.get(dataDir, String.format("sstable_%06d.data.tmp", generation));
        Path tempIndexPath = Paths.get(dataDir, String.format("sstable_%06d.index.tmp", generation));
        Path tempBloomPath = Paths.get(dataDir, String.format("sstable_%06d.bloom.tmp", generation));

        int entryCount = 0;
        String minKey = null;
        String maxKey = null;
        long dataOffset = 0;
        long totalFileSize = 0;

        TreeMap<String, KeyValuePair> sortedEntries = new TreeMap<>(memTable.getEntries());
        int expectedElements = Math.max(sortedEntries.size(), 1);
        BloomFilter bloomFilter = new BloomFilter(expectedElements, 0.01);

        // Create FileOutputStream references for sync operations
        FileOutputStream dbFileOut = null;
        FileOutputStream indexFileOut = null;
        FileOutputStream bloomFileOut = null;

        try (FileOutputStream dbFos = new FileOutputStream(tempDbPath.toFile());
             BufferedOutputStream dbBuf = new BufferedOutputStream(dbFos);
             DataOutputStream dbOut = new DataOutputStream(dbBuf);
             FileOutputStream indexFos = new FileOutputStream(tempIndexPath.toFile());
             BufferedOutputStream indexBuf = new BufferedOutputStream(indexFos);
             DataOutputStream indexOut = new DataOutputStream(indexBuf)) {

            dbFileOut = dbFos;
            indexFileOut = indexFos;

            for (Map.Entry<String, KeyValuePair> entry : sortedEntries.entrySet()) {
                KeyValuePair kvp = entry.getValue();
                String key = kvp.getKey();
                String value = kvp.getValue();

                // Track key range
                if (minKey == null || key.compareTo(minKey) < 0) {
                    minKey = key;
                }
                if (maxKey == null || key.compareTo(maxKey) > 0) {
                    maxKey = key;
                }

                // Add to bloom filter
                bloomFilter.add(key);

                // Write to data file
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = (value != null) ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
                byte isDeleted = (byte) (kvp.isDeleted() ? 1 : 0);

                // Write data entry
                dbOut.writeInt(keyBytes.length);
                dbOut.write(keyBytes);
                dbOut.writeInt(valueBytes.length);
                dbOut.write(valueBytes);
                dbOut.writeByte(isDeleted);
                dbOut.writeLong(kvp.getTimestamp());
                dbOut.writeInt(kvp.getGeneration());

                // Write index entry
                indexOut.writeInt(keyBytes.length);
                indexOut.write(keyBytes);
                indexOut.writeLong(dataOffset);

                // Calculate next offset
                long entrySize = 4 + keyBytes.length + 4 + valueBytes.length + 1 + 8 + 4;
                dataOffset += entrySize;
                totalFileSize += entrySize;
                entryCount++;
            }

            // Force synchronous write to ensure durability
            dbOut.flush();
            dbFileOut.getFD().sync();
            indexOut.flush();
            indexFileOut.getFD().sync();
        }

        // Write bloom filter
        try (FileOutputStream bloomFos = new FileOutputStream(tempBloomPath.toFile());
             BufferedOutputStream bloomBuf = new BufferedOutputStream(bloomFos);
             ObjectOutputStream bloomOut = new ObjectOutputStream(bloomBuf)) {

            bloomFileOut = bloomFos;
            bloomOut.writeObject(bloomFilter);
            bloomOut.flush();
            bloomFileOut.getFD().sync();
        }

        // Atomic move of temporary files
        Files.move(tempDbPath, dbPath, StandardCopyOption.ATOMIC_MOVE);
        Files.move(tempIndexPath, indexPath, StandardCopyOption.ATOMIC_MOVE);
        Files.move(tempBloomPath, bloomPath, StandardCopyOption.ATOMIC_MOVE);

        // Create and save metadata - using the correct constructor
        if (minKey == null) minKey = "";
        if (maxKey == null) maxKey = "";
        SSTableMetadata metadata = new SSTableMetadata(generation, 0, System.currentTimeMillis(),
                entryCount, minKey, maxKey, totalFileSize, totalFileSize, 0.01, null);
        metadata.saveToFile(metaPath);

        // Clear WAL after successful flush
        wal.clear();

        System.out.println("✅ SSTable gen-" + generation + " flushed successfully with " +
                entryCount + " entries, size: " + String.format("%.2fKB", totalFileSize / 1024.0));

        // Return SSTableReader for the new SSTable
        return new SSTableReader(dataDir, generation);
    }

    /**
     * Write compacted data to a new SSTable at the specified level.
     */
    public SSTableReader writeCompactedSSTable(Map<String, KeyValuePair> sortedEntries,
                                               int generation, int targetLevel) throws IOException {
        System.out.println("🔄 Writing compacted SSTable gen-" + generation + " at Level " + targetLevel);

        Path dbPath = Paths.get(dataDir, String.format("sstable_%06d.data", generation));
        Path indexPath = Paths.get(dataDir, String.format("sstable_%06d.index", generation));
        Path bloomPath = Paths.get(dataDir, String.format("sstable_%06d.bloom", generation));
        Path metaPath = Paths.get(dataDir, String.format("sstable_%06d.meta", generation));

        // Use temporary files for atomic writes
        Path tempDbPath = Paths.get(dataDir, String.format("sstable_%06d.data.tmp", generation));
        Path tempIndexPath = Paths.get(dataDir, String.format("sstable_%06d.index.tmp", generation));
        Path tempBloomPath = Paths.get(dataDir, String.format("sstable_%06d.bloom.tmp", generation));

        int entryCount = 0;
        String minKey = null;
        String maxKey = null;
        long dataOffset = 0;
        long totalFileSize = 0;

        int expectedElements = Math.max(sortedEntries.size(), 1);
        BloomFilter bloomFilter = new BloomFilter(expectedElements, 0.01);

        // Create FileOutputStream references for sync operations
        FileOutputStream dbFileOut = null;
        FileOutputStream indexFileOut = null;
        FileOutputStream bloomFileOut = null;

        try (FileOutputStream dbFos = new FileOutputStream(tempDbPath.toFile());
             BufferedOutputStream dbBuf = new BufferedOutputStream(dbFos);
             DataOutputStream dbOut = new DataOutputStream(dbBuf);
             FileOutputStream indexFos = new FileOutputStream(tempIndexPath.toFile());
             BufferedOutputStream indexBuf = new BufferedOutputStream(indexFos);
             DataOutputStream indexOut = new DataOutputStream(indexBuf)) {

            dbFileOut = dbFos;
            indexFileOut = indexFos;

            for (Map.Entry<String, KeyValuePair> entry : sortedEntries.entrySet()) {
                KeyValuePair kvp = entry.getValue();
                String key = kvp.getKey();
                String value = kvp.getValue();

                // Track key range
                if (minKey == null || key.compareTo(minKey) < 0) {
                    minKey = key;
                }
                if (maxKey == null || key.compareTo(maxKey) > 0) {
                    maxKey = key;
                }

                // Add to bloom filter
                bloomFilter.add(key);

                // Write to data file
                byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
                byte[] valueBytes = (value != null) ? value.getBytes(StandardCharsets.UTF_8) : new byte[0];
                byte isDeleted = (byte) (kvp.isDeleted() ? 1 : 0);

                // Write data entry
                dbOut.writeInt(keyBytes.length);
                dbOut.write(keyBytes);
                dbOut.writeInt(valueBytes.length);
                dbOut.write(valueBytes);
                dbOut.writeByte(isDeleted);
                dbOut.writeLong(kvp.getTimestamp());
                dbOut.writeInt(kvp.getGeneration());

                // Write index entry
                indexOut.writeInt(keyBytes.length);
                indexOut.write(keyBytes);
                indexOut.writeLong(dataOffset);

                // Calculate next offset
                long entrySize = 4 + keyBytes.length + 4 + valueBytes.length + 1 + 8 + 4;
                dataOffset += entrySize;
                totalFileSize += entrySize;
                entryCount++;
            }

            // Force synchronous write to ensure durability
            dbOut.flush();
            dbFileOut.getFD().sync();
            indexOut.flush();
            indexFileOut.getFD().sync();
        }

        // Write bloom filter
        try (FileOutputStream bloomFos = new FileOutputStream(tempBloomPath.toFile());
             BufferedOutputStream bloomBuf = new BufferedOutputStream(bloomFos);
             ObjectOutputStream bloomOut = new ObjectOutputStream(bloomBuf)) {

            bloomFileOut = bloomFos;
            bloomOut.writeObject(bloomFilter);
            bloomOut.flush();
            bloomFileOut.getFD().sync();
        }

        // Atomic move of temporary files
        Files.move(tempDbPath, dbPath, StandardCopyOption.ATOMIC_MOVE);
        Files.move(tempIndexPath, indexPath, StandardCopyOption.ATOMIC_MOVE);
        Files.move(tempBloomPath, bloomPath, StandardCopyOption.ATOMIC_MOVE);

        // Create and save metadata with target level - using the correct constructor
        if (minKey == null) minKey = "";
        if (maxKey == null) maxKey = "";
        SSTableMetadata metadata = new SSTableMetadata(generation, targetLevel, System.currentTimeMillis(),
                entryCount, minKey, maxKey, totalFileSize, totalFileSize, 0.01, null);
        metadata.saveToFile(metaPath);

        System.out.println("✅ Compacted SSTable gen-" + generation + " written successfully with " +
                entryCount + " entries at Level " + targetLevel +
                ", size: " + String.format("%.2fKB", totalFileSize / 1024.0));

        // Return SSTableReader for the new SSTable
        return new SSTableReader(dataDir, generation);
    }

    /**
     * Create multiple SSTables if the data is too large for a single SSTable.
     * This is useful for very large compactions.
     */
    public java.util.List<SSTableReader> writeCompactedSSTables(Map<String, KeyValuePair> sortedEntries,
                                                                int startGeneration, int targetLevel,
                                                                long maxSSTableSizeBytes) throws IOException {
        java.util.List<SSTableReader> newSSTables = new java.util.ArrayList<>();

        if (sortedEntries.size() == 0) {
            return newSSTables;
        }

        // If data fits in single SSTable, use regular method
        long estimatedSize = estimateDataSize(sortedEntries);
        if (estimatedSize <= maxSSTableSizeBytes) {
            newSSTables.add(writeCompactedSSTable(sortedEntries, startGeneration, targetLevel));
            return newSSTables;
        }

        // Split data across multiple SSTables
        Map<String, KeyValuePair> currentBatch = new TreeMap<>();
        long currentBatchSize = 0;
        int currentGeneration = startGeneration;

        for (Map.Entry<String, KeyValuePair> entry : sortedEntries.entrySet()) {
            KeyValuePair kvp = entry.getValue();
            long entrySize = estimateEntrySize(kvp);

            // If adding this entry would exceed the limit, write current batch
            if (currentBatchSize + entrySize > maxSSTableSizeBytes && !currentBatch.isEmpty()) {
                newSSTables.add(writeCompactedSSTable(currentBatch, currentGeneration++, targetLevel));
                currentBatch.clear();
                currentBatchSize = 0;
            }

            currentBatch.put(entry.getKey(), kvp);
            currentBatchSize += entrySize;
        }

        // Write final batch if not empty
        if (!currentBatch.isEmpty()) {
            newSSTables.add(writeCompactedSSTable(currentBatch, currentGeneration, targetLevel));
        }

        System.out.println("📚 Split compaction into " + newSSTables.size() + " SSTables");
        return newSSTables;
    }

    /**
     * Estimate the serialized size of all entries.
     */
    private long estimateDataSize(Map<String, KeyValuePair> entries) {
        long totalSize = 0;
        for (KeyValuePair kvp : entries.values()) {
            totalSize += estimateEntrySize(kvp);
        }
        return totalSize;
    }

    /**
     * Estimate the serialized size of a single entry.
     */
    private long estimateEntrySize(KeyValuePair kvp) {
        String key = kvp.getKey();
        String value = kvp.getValue();

        long keySize = key.getBytes(StandardCharsets.UTF_8).length;
        long valueSize = (value != null) ? value.getBytes(StandardCharsets.UTF_8).length : 0;

        // 4 (keyLen) + keySize + 4 (valueLen) + valueSize + 1 (isDeleted) + 8 (timestamp) + 4 (generation)
        return 4 + keySize + 4 + valueSize + 1 + 8 + 4;
    }

    /**
     * Clean up temporary files in case of errors.
     */
    private void cleanupTempFiles(int generation) {
        String[] extensions = {".data.tmp", ".index.tmp", ".bloom.tmp", ".meta.tmp"};
        for (String ext : extensions) {
            try {
                Path tempFile = Paths.get(dataDir, String.format("sstable_%06d%s", generation, ext));
                Files.deleteIfExists(tempFile);
            } catch (IOException e) {
                System.err.println("Warning: Could not delete temp file for gen-" + generation + ": " + e.getMessage());
            }
        }
    }
}