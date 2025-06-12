package org.example.lsm.core;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class LSMDatabase implements AutoCloseable {
    private static final int MEMTABLE_SIZE_THRESHOLD = 8;
    private static final long MEMTABLE_MEMORY_THRESHOLD = 1024 * 5;
    private final Object sstableLock = new Object();
    private final String dataDir;
    private final AtomicInteger generationCounter;
    public final CompactionManager compactionManager;
    private MemTable activeMemTable;
    private WAL activeWAL;
    private final List<SSTableReader> sstables;
    private final ExecutorService flushExecutor;
    private final Object flushLock = new Object();
    private int totalReads = 0;
    private int totalWrites = 0;
    private int totalDeletes = 0;
    private int flushCount = 0;

    public LSMDatabase(String dataDir) throws IOException {
        this.dataDir = dataDir;
        this.generationCounter = new AtomicInteger(0);
        this.sstables = new ArrayList<>();
        Files.createDirectories(Paths.get(dataDir));
        this.compactionManager = new CompactionManager(dataDir, generationCounter);
        initializeActiveMemTable();
        loadExistingSSTables();
        recoverFromWAL();
        this.flushExecutor = Executors.newSingleThreadExecutor();
    }

    private void initializeActiveMemTable() throws IOException {
        this.activeMemTable = new MemTable(MEMTABLE_MEMORY_THRESHOLD);

        // Handle existing WAL file - but don't try to rename it if it's currently open
        Path activeWalPath = Paths.get(dataDir, "active.wal");
        if (Files.exists(activeWalPath)) {
            // Instead of trying to rename the active WAL, we'll handle this differently
            // The existing WAL will be processed during recovery
            System.out.println("📝 Found existing WAL file: active.wal");
        }

        this.activeWAL = new WAL(dataDir, "active.wal");
        System.out.println("📝 Active WAL ready: active.wal");
    }

    private void loadExistingSSTables() {
        try {
            Path dir = Paths.get(dataDir);
            if (!Files.exists(dir)) return;

            Set<Integer> generations = new HashSet<>();
            Files.list(dir)
                    .filter(path -> path.getFileName().toString().matches("sstable_\\d{6}\\.db"))
                    .forEach(path -> {
                        String filename = path.getFileName().toString();
                        int generation = Integer.parseInt(filename.substring(8, 14));
                        generations.add(generation);
                    });

            synchronized (sstableLock) {
                for (Integer generation : generations) {
                    try {
                        SSTableReader reader = new SSTableReader(dataDir, generation);
                        sstables.add(reader);
                        compactionManager.addSSTable(reader);
                        generationCounter.updateAndGet(current -> Math.max(current, generation));
                        System.out.println("📚 Loaded existing SSTable gen-" + generation + " (Level " + reader.getLevel() + ")");
                    } catch (Exception e) {
                        System.err.println("⚠️  Failed to load SSTable generation " + generation + ": " + e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("⚠️  Error loading existing SSTables: " + e.getMessage());
        }
    }

    private void recoverFromWAL() throws IOException {
        activeWAL.recover(activeMemTable);
    }

    public void put(String key, String value) throws IOException {
        if (key == null || value == null) {
            throw new IllegalArgumentException("Key and value cannot be null");
        }
        System.out.println("📝 Writing to WAL: " + key + " = " + value);
        activeWAL.logPut(key, value);
        System.out.println("💾 Adding to MemTable: " + key + " = " + value);
        activeMemTable.put(key, value);
        totalWrites++;

        System.out.println("   MemTable size: " + activeMemTable.size() + "/" + MEMTABLE_SIZE_THRESHOLD +
                " entries, " + String.format("%.2fKB", activeMemTable.estimateMemoryUsage() / 1024.0) +
                " / " + String.format("%.2fKB", MEMTABLE_MEMORY_THRESHOLD / 1024.0));

        if (shouldFlush()) {
            System.out.println("🚨 MemTable threshold reached! Triggering flush...");
            triggerFlush();
        }
    }

    public void delete(String key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        System.out.println("📝 Writing DELETE to WAL: " + key);
        activeWAL.logDelete(key);
        System.out.println("⚰️  Adding tombstone to MemTable: " + key);
        activeMemTable.delete(key);
        totalDeletes++;

        System.out.println("   MemTable size: " + activeMemTable.size() + "/" + MEMTABLE_SIZE_THRESHOLD +
                " entries, " + String.format("%.2fKB", activeMemTable.estimateMemoryUsage() / 1024.0) +
                " / " + String.format("%.2fKB", MEMTABLE_MEMORY_THRESHOLD / 1024.0));
        if (shouldFlush()) {
            System.out.println("🚨 MemTable threshold reached! Triggering flush...");
            triggerFlush();
        }
    }

    private boolean shouldFlush() {
        return activeMemTable.size() >= MEMTABLE_SIZE_THRESHOLD ||
                activeMemTable.estimateMemoryUsage() >= MEMTABLE_MEMORY_THRESHOLD;
    }

    private void triggerFlush() {
        synchronized (flushLock) {
            if (!flushExecutor.isShutdown()) {
                flushExecutor.submit(() -> {
                    MemTable memTableToFlush;
                    WAL walToFlush;
                    int newGeneration;

                    synchronized (flushLock) {
                        memTableToFlush = this.activeMemTable;
                        walToFlush = this.activeWAL;
                        newGeneration = generationCounter.incrementAndGet();

                        try {
                            // Close the current WAL properly before creating a new one
                            System.out.println("🔄 Closing current WAL before flush...");
                            walToFlush.close();

                            // Now create new MemTable and WAL
                            this.activeMemTable = new MemTable(MEMTABLE_MEMORY_THRESHOLD);
                            this.activeWAL = new WAL(dataDir, "active.wal");
                            System.out.println("✅ New MemTable and WAL initialized successfully");

                        } catch (IOException e) {
                            System.err.println("CRITICAL: Failed to initialize new MemTable/WAL after flush: " + e.getMessage());
                            e.printStackTrace();
                            return;
                        }
                    }

                    try {
                        System.out.println("🔄 Starting flush of MemTable to SSTable gen-" + newGeneration);
                        SSTableWriter writer = new SSTableWriter(dataDir);
                        writer.flushToDisk(memTableToFlush, walToFlush, newGeneration);
                        SSTableReader newReader = new SSTableReader(dataDir, newGeneration);

                        synchronized (sstableLock) {
                            sstables.add(newReader);
                        }
                        compactionManager.addSSTable(newReader);

                        flushCount++;
                        System.out.println("✅ MemTable flush to SSTable gen-" + newGeneration + " completed.");

                        // Clear the WAL after successful flush
                        try {
                            walToFlush.clear();
                            System.out.println("🧹 Flushed WAL cleared successfully");
                        } catch (IOException e) {
                            System.err.println("Warning: Failed to clear flushed WAL: " + e.getMessage());
                        }

                    } catch (IOException e) {
                        System.err.println("CRITICAL: Failed to flush MemTable to SSTable: " + e.getMessage());
                        e.printStackTrace();
                    }
                });
            }
        }
    }

    public String get(String key) throws IOException {
        if (key == null) {
            return null;
        }

        totalReads++;
        System.out.println("\n🔍 Looking up key: " + key);

        System.out.println("  1. Checking MemTable...");
        KeyValuePair kvpFromMemTable = activeMemTable.getEntry(key);
        if (kvpFromMemTable != null) {
            if (kvpFromMemTable.isDeleted()) {
                System.out.println("  ⚰️  Found tombstone in MemTable (key deleted)");
                return null;
            } else {
                System.out.println("  ✅ Found in MemTable: " + kvpFromMemTable.getValue());
                return kvpFromMemTable.getValue();
            }
        }
        System.out.println("  ❌ Not found in MemTable.");

        List<SSTableReader> allSSTables = compactionManager.getAllSSTables();

        List<SSTableReader> sortedSSTablesForRead = new ArrayList<>(allSSTables);
        sortedSSTablesForRead.sort(Comparator.comparingInt(SSTableReader::getGeneration).reversed());

        for (int i = 0; i < sortedSSTablesForRead.size(); i++) {
            SSTableReader sstable = sortedSSTablesForRead.get(i);
            System.out.println("  " + (i + 2) + ". Checking SSTable gen-" + sstable.getGeneration() +
                    " (Level " + sstable.getLevel() + ", Range: " + sstable.getMinKey() + " to " + sstable.getMaxKey() + ")...");

            if (!sstable.mightContain(key)) {
                System.out.println("     🌸 Bloom filter says NO - skipping SSTable.");
                continue;
            }
            System.out.println("     🌸 Bloom filter says MAYBE - checking disk for key.");

            String value = String.valueOf(sstable.get(key));
            if (value != null) {
                System.out.println("  ✅ Found in SSTable gen-" + sstable.getGeneration() + ": " + value);
                return value;
            } else if (sstable.get(key) == null) {
                try {
                    Map<String, KeyValuePair> sstableKVP = sstable.scanAllKeyValuePairs();
                    KeyValuePair foundKVP = sstableKVP.get(key);
                    if (foundKVP != null && foundKVP.isDeleted()) {
                        System.out.println("  ⚰️  Found tombstone in SSTable gen-" + sstable.getGeneration() + " (key deleted)");
                        return null;
                    }
                } catch (IOException e) {
                    System.err.println("Error verifying tombstone in SSTable: " + e.getMessage());
                }
            }
            System.out.println("  ❌ Not found in this SSTable or was a tombstone.");
        }

        System.out.println("  ❌ Key not found anywhere.");
        return null;
    }

    public void insertRange(int start, int end) throws IOException {
        System.out.println("\n🔄 Inserting range " + start + " to " + end + " with random values...");
        Random random = new Random();

        for (int i = start; i <= end; i++) {
            String key = "key" + i;
            String value = "val" + random.nextInt(10000);
            put(key, value);

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            }
        }
        System.out.println("✅ Range insertion completed.");
    }

    public void printStatus() {
        System.out.println("\n" + "=".repeat(70));
        System.out.println("🌳 LSM-TREE STATUS");
        System.out.println("=".repeat(70));
        System.out.printf("📊 Statistics: %d reads, %d writes, %d deletes, %d flushes%n",
                totalReads, totalWrites, totalDeletes, flushCount);
        System.out.println("\n💾 MEMTABLE (Level -1, in-memory):");
        System.out.printf("  Size: %d/%d entries (%.1f%%)%n",
                activeMemTable.size(), MEMTABLE_SIZE_THRESHOLD,
                (activeMemTable.size() * 100.0) / MEMTABLE_SIZE_THRESHOLD);
        System.out.printf("  Memory: %.2f KB / %.2f KB%n",
                activeMemTable.estimateMemoryUsage() / 1024.0, MEMTABLE_MEMORY_THRESHOLD / 1024.0);

        if (activeMemTable.size() > 0) {
            System.out.println("  Entries (first 5):");
            int count = 0;
            for (Map.Entry<String, KeyValuePair> entry : activeMemTable.getEntries().entrySet()) {
                if (count++ >= 5) {
                    System.out.println("    ... and " + (activeMemTable.size() - 5) + " more");
                    break;
                }
                KeyValuePair kvp = entry.getValue();
                String status = kvp.isDeleted() ? " [TOMBSTONE]" : "";
                System.out.printf("    '%s' = '%s'%s (Gen: %d)%n", kvp.getKey(),
                        kvp.isDeleted() ? "<deleted>" : kvp.getValue(), status, kvp.getGeneration());
            }
        } else {
            System.out.println("  MemTable is empty.");
        }

        System.out.println("\n🗃️  SSTABLES (on disk):");
        boolean anySSTables = false;
        for (int level = 0; level <= CompactionManager.MAX_LEVEL; level++) {
            List<SSTableReader> levelSSTables = compactionManager.getSSTablesForLevel(level);
            if (!levelSSTables.isEmpty()) {
                anySSTables = true;
                System.out.printf("  Level %d (%d files):%n", level, levelSSTables.size());
                levelSSTables.sort(Comparator.comparingInt(SSTableReader::getGeneration));
                for (SSTableReader sstable : levelSSTables) {
                    System.out.printf("    - Gen-%d: [%s - %s] (%d entries)%n",
                            sstable.getGeneration(), sstable.getMinKey(), sstable.getMaxKey(), sstable.metadata.getEntryCount());
                }
            }
        }
        if (!anySSTables) {
            System.out.println("  No SSTables on disk yet.");
        }

        System.out.println("=".repeat(70) + "\n");
    }

    public static class MemTable {
        private final TreeMap<String, KeyValuePair> entries;
        private final long memoryThresholdBytes;
        private long currentMemoryUsage;

        public MemTable(long memoryThresholdBytes) {
            this.entries = new TreeMap<>();
            this.memoryThresholdBytes = memoryThresholdBytes;
            this.currentMemoryUsage = 0;
        }

        public void put(String key, String value) {
            KeyValuePair newKVP = new KeyValuePair(key, value);
            KeyValuePair oldKVP = entries.put(key, newKVP);
            updateMemoryUsage(newKVP, oldKVP);
        }

        public void delete(String key) {
            KeyValuePair newKVP = KeyValuePair.tombstone(key);
            KeyValuePair oldKVP = entries.put(key, newKVP);
            updateMemoryUsage(newKVP, oldKVP);
        }

        public String get(String key) {
            KeyValuePair kvp = entries.get(key);
            if (kvp != null && kvp.isDeleted()) {
                return null;
            }
            return (kvp != null) ? kvp.getValue() : null;
        }

        public KeyValuePair getEntry(String key) {
            return entries.get(key);
        }

        public boolean isDeleted(String key) {
            KeyValuePair kvp = entries.get(key);
            return kvp != null && kvp.isDeleted();
        }

        public int size() {
            return entries.size();
        }

        public TreeMap<String, KeyValuePair> getEntries() {
            return entries;
        }

        private void updateMemoryUsage(KeyValuePair newKVP, KeyValuePair oldKVP) {
            if (oldKVP != null) {
                currentMemoryUsage -= oldKVP.estimateSize();
            }
            currentMemoryUsage += newKVP.estimateSize();
        }

        public long estimateMemoryUsage() {
            return currentMemoryUsage;
        }
    }

    private int getSSTableLevel(SSTableReader sstable) {
        try {
            Path metaPath = Paths.get(dataDir, String.format("sstable_%06d.meta", sstable.getGeneration()));
            if (Files.exists(metaPath)) {
                SSTableMetadata metadata = SSTableMetadata.loadFromFile(metaPath);
                return metadata.getLevel();
            }
        } catch (IOException e) {
            System.err.println("Error getting SSTable level for status print: " + e.getMessage());
        }
        return 0;
    }

    @Override
    public void close() throws Exception {
        System.out.println("\n🚫 Shutting down database...");
        if (activeMemTable.size() > 0) {
            System.out.println("Final flush of active MemTable before shutdown...");
            triggerFlush();
            flushExecutor.shutdown();
            if (!flushExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                System.err.println("Flush executor did not terminate in time. Forcing shutdown.");
                flushExecutor.shutdownNow();
            }
        } else {
            flushExecutor.shutdown();
        }

        if (activeWAL != null) {
            activeWAL.close();
        }

        compactionManager.shutdown();

        System.out.println("✅ Database shutdown complete.");
    }
}