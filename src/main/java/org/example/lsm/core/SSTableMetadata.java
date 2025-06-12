package org.example.lsm.core;

import java.io.*;
import java.nio.file.*;
import java.util.Properties;

/**
 * Enhanced SSTableMetadata with comprehensive tracking of SSTable properties
 * - Tracks file size, creation time, and level information
 * - Supports key range queries and overlap detection
 * - Thread-safe metadata operations
 */
public class SSTableMetadata implements Serializable {
    private static final long serialVersionUID = 3L;

    private final int generation;
    private final int level;
    private final long creationTimestamp;
    private final int entryCount;
    private final String minKey;
    private final String maxKey;
    private final long fileSize;
    private final long totalBytes;
    private final double bloomFilterFalsePositiveRate;
    private final String checksum;

    public SSTableMetadata(int generation, int level, long creationTimestamp,
                           int entryCount, String minKey, String maxKey,
                           long fileSize, long totalBytes,
                           double bloomFilterFalsePositiveRate, String checksum) {
        this.generation = generation;
        this.level = level;
        this.creationTimestamp = creationTimestamp;
        this.entryCount = entryCount;
        this.minKey = minKey != null ? minKey : "";
        this.maxKey = maxKey != null ? maxKey : "";
        this.fileSize = fileSize;
        this.totalBytes = totalBytes;
        this.bloomFilterFalsePositiveRate = bloomFilterFalsePositiveRate;
        this.checksum = checksum != null ? checksum : "";
    }

    // Backward compatibility constructor
    public SSTableMetadata(int generation, int level, long creationTimestamp,
                           int entryCount, String minKey, String maxKey) {
        this(generation, level, creationTimestamp, entryCount, minKey, maxKey,
                0L, 0L, 0.01, null);
    }

    // Getters
    public int getGeneration() { return generation; }
    public int getLevel() { return level; }
    public long getCreationTimestamp() { return creationTimestamp; }
    public int getEntryCount() { return entryCount; }
    public String getMinKey() { return minKey; }
    public String getMaxKey() { return maxKey; }
    public long getFileSize() { return fileSize; }
    public long getTotalBytes() { return totalBytes; }
    public double getBloomFilterFalsePositiveRate() { return bloomFilterFalsePositiveRate; }
    public String getChecksum() { return checksum; }

    /**
     * Check if this SSTable's key range overlaps with another
     */
    public boolean overlaps(SSTableMetadata other) {
        if (minKey.isEmpty() || maxKey.isEmpty() ||
                other.minKey.isEmpty() || other.maxKey.isEmpty()) {
            return true; // Conservative approach - assume overlap if ranges unknown
        }
        return !(maxKey.compareTo(other.minKey) < 0 || other.maxKey.compareTo(minKey) < 0);
    }

    /**
     * Check if this SSTable might contain the given key
     */
    public boolean mightContainKey(String key) {
        if (minKey.isEmpty() || maxKey.isEmpty()) {
            return true; // Conservative approach
        }
        return key.compareTo(minKey) >= 0 && key.compareTo(maxKey) <= 0;
    }

    /**
     * Check if this SSTable's key range overlaps with the given range
     */
    public boolean overlapsRange(String startKey, String endKey) {
        if (minKey.isEmpty() || maxKey.isEmpty()) {
            return true; // Conservative approach
        }
        return !(maxKey.compareTo(startKey) < 0 || endKey.compareTo(minKey) < 0);
    }

    /**
     * Get the estimated density (entries per byte)
     */
    public double getDensity() {
        return totalBytes > 0 ? (double) entryCount / totalBytes : 0.0;
    }

    /**
     * Get age in milliseconds
     */
    public long getAgeMillis() {
        return System.currentTimeMillis() - creationTimestamp;
    }

    /**
     * Create a new metadata instance with updated level
     */
    public SSTableMetadata withLevel(int newLevel) {
        return new SSTableMetadata(generation, newLevel, creationTimestamp,
                entryCount, minKey, maxKey, fileSize, totalBytes,
                bloomFilterFalsePositiveRate, checksum);
    }

    /**
     * Save metadata to file using Properties format
     */
    public void saveToFile(Path filePath) throws IOException {
        Properties props = new Properties();
        props.setProperty("generation", String.valueOf(generation));
        props.setProperty("level", String.valueOf(level));
        props.setProperty("creationTimestamp", String.valueOf(creationTimestamp));
        props.setProperty("entryCount", String.valueOf(entryCount));
        props.setProperty("minKey", minKey);
        props.setProperty("maxKey", maxKey);
        props.setProperty("fileSize", String.valueOf(fileSize));
        props.setProperty("totalBytes", String.valueOf(totalBytes));
        props.setProperty("bloomFilterFalsePositiveRate", String.valueOf(bloomFilterFalsePositiveRate));
        props.setProperty("checksum", checksum);

        // Create parent directories if they don't exist
        Files.createDirectories(filePath.getParent());

        // Atomic write using temporary file
        Path tempPath = filePath.resolveSibling(filePath.getFileName() + ".tmp");
        try (OutputStream out = Files.newOutputStream(tempPath,
                StandardOpenOption.CREATE,
                StandardOpenOption.WRITE,
                StandardOpenOption.TRUNCATE_EXISTING)) {
            props.store(out, "SSTable Metadata for generation " + generation);
        }

        // Atomic move
        Files.move(tempPath, filePath, StandardCopyOption.ATOMIC_MOVE);
    }

    /**
     * Load metadata from file
     */
    public static SSTableMetadata loadFromFile(Path filePath) throws IOException {
        if (!Files.exists(filePath)) {
            throw new FileNotFoundException("Metadata file not found: " + filePath);
        }

        Properties props = new Properties();
        try (InputStream in = Files.newInputStream(filePath)) {
            props.load(in);
        }

        int generation = Integer.parseInt(props.getProperty("generation", "0"));
        int level = Integer.parseInt(props.getProperty("level", "0"));
        long creationTimestamp = Long.parseLong(props.getProperty("creationTimestamp", "0"));
        int entryCount = Integer.parseInt(props.getProperty("entryCount", "0"));
        String minKey = props.getProperty("minKey", "");
        String maxKey = props.getProperty("maxKey", "");
        long fileSize = Long.parseLong(props.getProperty("fileSize", "0"));
        long totalBytes = Long.parseLong(props.getProperty("totalBytes", "0"));
        double bloomFilterFalsePositiveRate = Double.parseDouble(
                props.getProperty("bloomFilterFalsePositiveRate", "0.01"));
        String checksum = props.getProperty("checksum", "");

        return new SSTableMetadata(generation, level, creationTimestamp, entryCount,
                minKey, maxKey, fileSize, totalBytes,
                bloomFilterFalsePositiveRate, checksum);
    }

    @Override
    public String toString() {
        return String.format("SSTableMetadata{gen=%d, level=%d, entries=%d, " +
                        "range='%s'->'%s', size=%.2fKB, age=%dms}",
                generation, level, entryCount,
                minKey.length() > 10 ? minKey.substring(0, 10) + "..." : minKey,
                maxKey.length() > 10 ? maxKey.substring(0, 10) + "..." : maxKey,
                fileSize / 1024.0, getAgeMillis());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        SSTableMetadata that = (SSTableMetadata) obj;
        return generation == that.generation;
    }

    @Override
    public int hashCode() {
        return Integer.hashCode(generation);
    }
}