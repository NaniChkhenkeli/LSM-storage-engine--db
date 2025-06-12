package org.example.lsm.core;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.*;
import java.util.stream.Collectors;

/**
 * CompactionManager handles background compaction using a hybrid strategy:
 * - Size-tiered compaction for Level 0 (merge multiple SSTables when threshold reached)
 * - Leveled compaction for higher levels (maintain sorted order and size limits)
 */
public class CompactionManager implements AutoCloseable {
    public static final int MAX_LEVEL = 7;
    private static final int L0_COMPACTION_THRESHOLD = 4; // Trigger compaction when L0 has 4+ SSTables
    private static final long LEVEL_SIZE_MULTIPLIER = 10; // Each level is 10x larger than previous
    private static final long BASE_LEVEL_SIZE = 10 * 1024 * 1024; // 10MB for Level 1

    private final String dataDir;
    private final AtomicInteger generationCounter;
    private final ExecutorService compactionExecutor;
    private final ScheduledExecutorService scheduledExecutor;
    private volatile boolean shutdown = false;

    // Level-based SSTable organization
    private final Map<Integer, List<SSTableReader>> levelToSSTables;
    private final ReentrantReadWriteLock levelsLock = new ReentrantReadWriteLock();

    // Compaction statistics
    private final AtomicLong totalCompactions = new AtomicLong(0);
    private final AtomicLong totalSSTablesCompacted = new AtomicLong(0);
    private final AtomicLong totalBytesCompacted = new AtomicLong(0);
    private volatile long lastCompactionTime = 0;

    // Active compactions tracking
    private final Set<CompactionTask> activeCompactions = ConcurrentHashMap.newKeySet();

    public CompactionManager(String dataDir, AtomicInteger generationCounter) {
        this.dataDir = dataDir;
        this.generationCounter = generationCounter;
        this.levelToSSTables = new ConcurrentHashMap<>();

        // Initialize levels
        for (int level = 0; level <= MAX_LEVEL; level++) {
            levelToSSTables.put(level, new ArrayList<>());
        }

        // Create thread pool for background compactions
        this.compactionExecutor = Executors.newFixedThreadPool(2, r -> {
            Thread t = new Thread(r, "Compaction-Worker");
            t.setDaemon(true);
            return t;
        });

        // Scheduled executor for periodic compaction checks
        this.scheduledExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Compaction-Scheduler");
            t.setDaemon(true);
            return t;
        });

        // Start periodic compaction checker
        scheduledExecutor.scheduleWithFixedDelay(this::checkAndTriggerCompactions,
                10, 30, TimeUnit.SECONDS);

        System.out.println("🔧 CompactionManager initialized with hybrid compaction strategy");
    }

    /**
     * Add a new SSTable to the appropriate level.
     * New SSTables from MemTable flushes go to Level 0.
     */
    public void addSSTable(SSTableReader sstable) {
        int level = sstable.getLevel();

        levelsLock.writeLock().lock();
        try {
            List<SSTableReader> levelSSTables = levelToSSTables.get(level);
            if (levelSSTables != null) {
                levelSSTables.add(sstable);
                System.out.println("📚 Added SSTable gen-" + sstable.getGeneration() +
                        " to Level " + level + " (" + levelSSTables.size() + " total)");
            }
        } finally {
            levelsLock.writeLock().unlock();
        }

        // Check if compaction is needed after adding
        checkAndTriggerCompactions();
    }

    /**
     * Remove SSTable from tracking (used during compaction).
     */
    public void removeSSTable(SSTableReader sstable) {
        levelsLock.writeLock().lock();
        try {
            for (List<SSTableReader> levelSSTables : levelToSSTables.values()) {
                levelSSTables.remove(sstable);
            }
        } finally {
            levelsLock.writeLock().unlock();
        }
    }

    /**
     * Get all SSTables across all levels, sorted by generation (newest first).
     */
    public List<SSTableReader> getAllSSTables() {
        levelsLock.readLock().lock();
        try {
            List<SSTableReader> all = new ArrayList<>();
            for (List<SSTableReader> levelSSTables : levelToSSTables.values()) {
                all.addAll(levelSSTables);
            }
            // Sort by generation descending (newest first for reads)
            all.sort(Comparator.comparingInt(SSTableReader::getGeneration).reversed());
            return all;
        } finally {
            levelsLock.readLock().unlock();
        }
    }

    /**
     * Get SSTables for a specific level.
     */
    public List<SSTableReader> getSSTablesForLevel(int level) {
        levelsLock.readLock().lock();
        try {
            return new ArrayList<>(levelToSSTables.getOrDefault(level, new ArrayList<>()));
        } finally {
            levelsLock.readLock().unlock();
        }
    }

    /**
     * Check all levels and trigger compactions if needed.
     */
    private void checkAndTriggerCompactions() {
        if (shutdown) return;

        // Check Level 0 first (size-tiered compaction)
        checkLevel0Compaction();

        // Check higher levels (leveled compaction)
        for (int level = 1; level <= MAX_LEVEL - 1; level++) {
            checkLeveledCompaction(level);
        }
    }

    /**
     * Check if Level 0 needs compaction (size-tiered approach).
     */
    private void checkLevel0Compaction() {
        levelsLock.readLock().lock();
        List<SSTableReader> level0SSTables;
        try {
            level0SSTables = new ArrayList<>(levelToSSTables.get(0));
        } finally {
            levelsLock.readLock().unlock();
        }

        if (level0SSTables.size() >= L0_COMPACTION_THRESHOLD) {
            System.out.println("🔄 Level 0 compaction triggered: " + level0SSTables.size() +
                    " SSTables >= threshold " + L0_COMPACTION_THRESHOLD);

            CompactionTask task = new CompactionTask(
                    CompactionType.SIZE_TIERED, 0, level0SSTables, 1);

            if (activeCompactions.add(task)) {
                compactionExecutor.submit(() -> executeCompaction(task));
            }
        }
    }

    /**
     * Check if a level needs leveled compaction.
     */
    private void checkLeveledCompaction(int level) {
        levelsLock.readLock().lock();
        List<SSTableReader> currentLevel, nextLevel;
        try {
            currentLevel = new ArrayList<>(levelToSSTables.get(level));
            nextLevel = new ArrayList<>(levelToSSTables.get(level + 1));
        } finally {
            levelsLock.readLock().unlock();
        }

        long currentLevelSize = currentLevel.stream()
                .mapToLong(s -> s.metadata.getFileSize())
                .sum();

        long maxLevelSize = BASE_LEVEL_SIZE * (long) Math.pow(LEVEL_SIZE_MULTIPLIER, level - 1);

        if (currentLevelSize > maxLevelSize) {
            System.out.println("🔄 Level " + level + " compaction triggered: " +
                    String.format("%.2fMB", currentLevelSize / (1024.0 * 1024.0)) +
                    " > " + String.format("%.2fMB", maxLevelSize / (1024.0 * 1024.0)));

            // Select SSTable with overlapping key ranges in next level
            List<SSTableReader> toCompact = selectSSTablesForLeveledCompaction(currentLevel, nextLevel);

            if (!toCompact.isEmpty()) {
                CompactionTask task = new CompactionTask(
                        CompactionType.LEVELED, level, toCompact, level + 1);

                if (activeCompactions.add(task)) {
                    compactionExecutor.submit(() -> executeCompaction(task));
                }
            }
        }
    }

    /**
     * Select SSTables for leveled compaction based on key range overlaps.
     */
    private List<SSTableReader> selectSSTablesForLeveledCompaction(
            List<SSTableReader> currentLevel, List<SSTableReader> nextLevel) {

        if (currentLevel.isEmpty()) return new ArrayList<>();

        // Pick the SSTable with the most overlaps in the next level
        SSTableReader bestCandidate = null;
        int maxOverlaps = 0;

        for (SSTableReader candidate : currentLevel) {
            int overlaps = 0;
            for (SSTableReader nextLevelTable : nextLevel) {
                if (keyRangesOverlap(candidate, nextLevelTable)) {
                    overlaps++;
                }
            }

            if (overlaps > maxOverlaps) {
                maxOverlaps = overlaps;
                bestCandidate = candidate;
            }
        }

        if (bestCandidate == null) {
            return new ArrayList<>();
        }

        // Include the selected SSTable and all overlapping SSTables from next level
        List<SSTableReader> toCompact = new ArrayList<>();
        toCompact.add(bestCandidate);

        for (SSTableReader nextLevelTable : nextLevel) {
            if (keyRangesOverlap(bestCandidate, nextLevelTable)) {
                toCompact.add(nextLevelTable);
            }
        }

        return toCompact;
    }

    /**
     * Check if two SSTables have overlapping key ranges.
     */
    private boolean keyRangesOverlap(SSTableReader a, SSTableReader b) {
        String aMin = a.getMinKey(), aMax = a.getMaxKey();
        String bMin = b.getMinKey(), bMax = b.getMaxKey();

        return !(aMax.compareTo(bMin) < 0 || bMax.compareTo(aMin) < 0);
    }

    /**
     * Execute a compaction task.
     */
    private void executeCompaction(CompactionTask task) {
        long startTime = System.currentTimeMillis();

        try {
            System.out.println("🔄 Starting " + task.type + " compaction: Level " +
                    task.sourceLevel + " -> Level " + task.targetLevel +
                    " (" + task.sstables.size() + " SSTables)");

            // Remove SSTables from their current levels
            levelsLock.writeLock().lock();
            try {
                for (SSTableReader sstable : task.sstables) {
                    removeSSTable(sstable);
                }
            } finally {
                levelsLock.writeLock().unlock();
            }

            // Perform the actual compaction
            List<SSTableReader> newSSTables = performCompaction(task);

            // Add new SSTables to target level
            levelsLock.writeLock().lock();
            try {
                List<SSTableReader> targetLevelSSTables = levelToSSTables.get(task.targetLevel);
                targetLevelSSTables.addAll(newSSTables);
            } finally {
                levelsLock.writeLock().unlock();
            }

            // Clean up old SSTable files
            for (SSTableReader oldSSTable : task.sstables) {
                try {
                    oldSSTable.close();
                    deleteSSTableFiles(oldSSTable.getGeneration());
                } catch (Exception e) {
                    System.err.println("Error cleaning up old SSTable gen-" +
                            oldSSTable.getGeneration() + ": " + e.getMessage());
                }
            }

            // Update statistics
            totalCompactions.incrementAndGet();
            totalSSTablesCompacted.addAndGet(task.sstables.size());
            totalBytesCompacted.addAndGet(task.sstables.stream()
                    .mapToLong(s -> s.metadata.getFileSize()).sum());
            lastCompactionTime = System.currentTimeMillis();

            long duration = System.currentTimeMillis() - startTime;
            System.out.println("✅ Compaction completed in " + duration + "ms. " +
                    "Created " + newSSTables.size() + " new SSTables at Level " +
                    task.targetLevel);

        } catch (Exception e) {
            System.err.println("❌ Compaction failed: " + e.getMessage());
            e.printStackTrace();

            // Re-add SSTables to their original levels on failure
            levelsLock.writeLock().lock();
            try {
                for (SSTableReader sstable : task.sstables) {
                    List<SSTableReader> levelSSTables = levelToSSTables.get(sstable.getLevel());
                    if (!levelSSTables.contains(sstable)) {
                        levelSSTables.add(sstable);
                    }
                }
            } finally {
                levelsLock.writeLock().unlock();
            }
        } finally {
            activeCompactions.remove(task);
        }
    }

    /**
     * Perform the actual compaction by merging SSTables.
     */
    private List<SSTableReader> performCompaction(CompactionTask task) throws IOException {
        // Collect all key-value pairs from all SSTables
        Map<String, KeyValuePair> allEntries = new TreeMap<>();

        for (SSTableReader sstable : task.sstables) {
            Map<String, KeyValuePair> entries = sstable.scanAllKeyValuePairs();

            for (Map.Entry<String, KeyValuePair> entry : entries.entrySet()) {
                String key = entry.getKey();
                KeyValuePair newKVP = entry.getValue();
                KeyValuePair existingKVP = allEntries.get(key);

                // Use MVCC: keep the version with higher generation/timestamp
                if (existingKVP == null || isNewerVersion(newKVP, existingKVP)) {
                    allEntries.put(key, newKVP);
                }
            }
        }

        // Remove tombstones (deleted entries) - they've served their purpose
        allEntries.entrySet().removeIf(entry -> entry.getValue().isDeleted());

        if (allEntries.isEmpty()) {
            System.out.println("   No entries to write after compaction (all were tombstones)");
            return new ArrayList<>();
        }

        // Write compacted entries to new SSTable(s)
        List<SSTableReader> newSSTables = new ArrayList<>();
        int newGeneration = generationCounter.incrementAndGet();

        SSTableWriter writer = new SSTableWriter(dataDir);
        SSTableReader newSSTable = writer.writeCompactedSSTable(allEntries, newGeneration, task.targetLevel);
        newSSTables.add(newSSTable);

        System.out.println("   Created compacted SSTable gen-" + newGeneration +
                " with " + allEntries.size() + " entries at Level " + task.targetLevel);

        return newSSTables;
    }

    /**
     * Check if one KeyValuePair is newer than another using MVCC.
     */
    private boolean isNewerVersion(KeyValuePair a, KeyValuePair b) {
        // Higher generation wins
        if (a.getGeneration() != b.getGeneration()) {
            return a.getGeneration() > b.getGeneration();
        }
        // If same generation, higher timestamp wins
        return a.getTimestamp() > b.getTimestamp();
    }

    /**
     * Delete all files associated with an SSTable generation.
     */
    private void deleteSSTableFiles(int generation) {
        try {
            String[] extensions = {".data", ".index", ".bloom", ".meta"};
            for (String ext : extensions) {
                Path file = Paths.get(dataDir, String.format("sstable_%06d%s", generation, ext));
                Files.deleteIfExists(file);
            }
            System.out.println("🗑️  Deleted SSTable gen-" + generation + " files");
        } catch (IOException e) {
            System.err.println("Error deleting SSTable files for gen-" + generation + ": " + e.getMessage());
        }
    }

    /**
     * Get compaction statistics.
     */
    public CompactionStats getStats() {
        levelsLock.readLock().lock();
        try {
            Map<Integer, Integer> levelCounts = new HashMap<>();
            Map<Integer, Long> levelSizes = new HashMap<>();

            for (int level = 0; level <= MAX_LEVEL; level++) {
                List<SSTableReader> levelSSTables = levelToSSTables.get(level);
                levelCounts.put(level, levelSSTables.size());
                levelSizes.put(level, levelSSTables.stream()
                        .mapToLong(s -> s.metadata.getFileSize()).sum());
            }

            return new CompactionStats(
                    totalCompactions.get(),
                    totalSSTablesCompacted.get(),
                    totalBytesCompacted.get(),
                    lastCompactionTime,
                    activeCompactions.size(),
                    levelCounts,
                    levelSizes
            );
        } finally {
            levelsLock.readLock().unlock();
        }
    }

    /**
     * Force compaction of a specific level (for testing/maintenance).
     */
    public void forceCompaction(int level) {
        if (level == 0) {
            checkLevel0Compaction();
        } else {
            checkLeveledCompaction(level);
        }
    }

    @Override
    public void close() throws Exception {
        System.out.println("🚫 Shutting down CompactionManager...");
        shutdown = true;

        scheduledExecutor.shutdown();
        compactionExecutor.shutdown();

        try {
            if (!compactionExecutor.awaitTermination(30, TimeUnit.SECONDS)) {
                System.err.println("⚠️  Compaction threads did not terminate gracefully");
                compactionExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            compactionExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }

        // Close all SSTables
        levelsLock.writeLock().lock();
        try {
            for (List<SSTableReader> levelSSTables : levelToSSTables.values()) {
                for (SSTableReader sstable : levelSSTables) {
                    try {
                        sstable.close();
                    } catch (Exception e) {
                        System.err.println("Error closing SSTable: " + e.getMessage());
                    }
                }
                levelSSTables.clear();
            }
        } finally {
            levelsLock.writeLock().unlock();
        }

        System.out.println("✅ CompactionManager shutdown complete");
    }

    // Helper classes

    private enum CompactionType {
        SIZE_TIERED, LEVELED
    }

    private static class CompactionTask {
        final CompactionType type;
        final int sourceLevel;
        final List<SSTableReader> sstables;
        final int targetLevel;

        CompactionTask(CompactionType type, int sourceLevel, List<SSTableReader> sstables, int targetLevel) {
            this.type = type;
            this.sourceLevel = sourceLevel;
            this.sstables = new ArrayList<>(sstables);
            this.targetLevel = targetLevel;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            CompactionTask that = (CompactionTask) o;
            return sourceLevel == that.sourceLevel &&
                    targetLevel == that.targetLevel &&
                    type == that.type &&
                    Objects.equals(sstables, that.sstables);
        }

        @Override
        public int hashCode() {
            return Objects.hash(type, sourceLevel, sstables, targetLevel);
        }
    }

    public static class CompactionStats {
        public final long totalCompactions;
        public final long totalSSTablesCompacted;
        public final long totalBytesCompacted;
        public final long lastCompactionTime;
        public final int activeCompactions;
        public final Map<Integer, Integer> levelCounts;
        public final Map<Integer, Long> levelSizes;

        CompactionStats(long totalCompactions, long totalSSTablesCompacted,
                        long totalBytesCompacted, long lastCompactionTime,
                        int activeCompactions, Map<Integer, Integer> levelCounts,
                        Map<Integer, Long> levelSizes) {
            this.totalCompactions = totalCompactions;
            this.totalSSTablesCompacted = totalSSTablesCompacted;
            this.totalBytesCompacted = totalBytesCompacted;
            this.lastCompactionTime = lastCompactionTime;
            this.activeCompactions = activeCompactions;
            this.levelCounts = new HashMap<>(levelCounts);
            this.levelSizes = new HashMap<>(levelSizes);
        }
    }

    public void shutdown() {
        try {
            close();
        } catch (Exception e) {
            System.err.println("Error during CompactionManager shutdown: " + e.getMessage());
        }
    }
}