package org.example.lsm.core;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class WAL {
    private final String dataDir;
    private final Path walPath;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
    private FileOutputStream fileStream;
    private BufferedOutputStream bufferedStream;
    private DataOutputStream outputStream;
    private long walSequenceNumber = 0;
    private boolean closed = false;

    // Constructor that matches LSMDatabase usage: WAL(dataDir, filename)
    public WAL(String dataDir, String filename) throws IOException {
        this.dataDir = dataDir;
        this.walPath = Paths.get(dataDir, filename);

        // Create directory if it doesn't exist
        Files.createDirectories(Paths.get(dataDir));

        // Open WAL file for append
        initializeStreams();
    }

    // Legacy constructor for backward compatibility
    public WAL(String dataDir) throws IOException {
        this(dataDir, "wal.log");
    }

    private void initializeStreams() throws IOException {
        this.fileStream = new FileOutputStream(walPath.toFile(), true);
        this.bufferedStream = new BufferedOutputStream(fileStream);
        this.outputStream = new DataOutputStream(bufferedStream);
    }

    /**
     * Logs a PUT operation to the WAL - matches LSMDatabase.logPut() call
     */
    public void logPut(String key, String value) throws IOException {
        logWrite(key, value);
    }

    /**
     * Logs a write operation to the WAL with synchronous disk write for durability
     */
    public void logWrite(String key, String value) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        lock.writeLock().lock();
        try {
            if (closed) {
                throw new IOException("WAL is closed");
            }

            writeLogEntry(LogEntry.LogType.PUT, key, value, System.nanoTime());
            outputStream.flush();
            // Force synchronous write to disk for durability
            fileStream.getFD().sync();
            walSequenceNumber++;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Logs a delete operation to the WAL
     */
    public void logDelete(String key) throws IOException {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }

        lock.writeLock().lock();
        try {
            if (closed) {
                throw new IOException("WAL is closed");
            }

            writeLogEntry(LogEntry.LogType.DELETE, key, null, System.nanoTime());
            outputStream.flush();
            fileStream.getFD().sync();
            walSequenceNumber++;
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void writeLogEntry(LogEntry.LogType type, String key, String value, long timestamp) throws IOException {
        // Write log entry format: [type][timestamp][key_len][key][value_len][value]
        outputStream.writeByte(type.ordinal());
        outputStream.writeLong(timestamp);

        byte[] keyBytes = key.getBytes(StandardCharsets.UTF_8);
        outputStream.writeInt(keyBytes.length);
        outputStream.write(keyBytes);

        if (value != null) {
            byte[] valueBytes = value.getBytes(StandardCharsets.UTF_8);
            outputStream.writeInt(valueBytes.length);
            outputStream.write(valueBytes);
        } else {
            outputStream.writeInt(0);
        }
    }

    /**
     * Recovers operations from WAL and applies them to the given MemTable
     * This matches the LSMDatabase.recoverFromWAL() usage pattern
     */
    public void recover(LSMDatabase.MemTable memTable) throws IOException {
        List<LogEntry> entries = recover();
        System.out.println("🔄 Applying " + entries.size() + " WAL entries to MemTable");

        for (LogEntry entry : entries) {
            switch (entry.getType()) {
                case PUT:
                    memTable.put(entry.getKey(), entry.getValue());
                    System.out.println("  ↻ Recovered PUT: " + entry.getKey() + " = " + entry.getValue());
                    break;
                case DELETE:
                    memTable.delete(entry.getKey());
                    System.out.println("  ↻ Recovered DELETE: " + entry.getKey());
                    break;
            }
        }

        if (entries.size() > 0) {
            System.out.println("✅ WAL recovery completed");
        }
    }

    /**
     * Recovers all operations from the WAL file
     */
    public List<LogEntry> recover() throws IOException {
        lock.readLock().lock();
        try {
            List<LogEntry> entries = new ArrayList<>();

            if (!Files.exists(walPath)) {
                System.out.println("📁 No WAL file found, starting with empty log");
                return entries;
            }

            System.out.println("🔄 Recovering from WAL: " + walPath);

            try (DataInputStream input = new DataInputStream(new BufferedInputStream(Files.newInputStream(walPath)))) {
                while (input.available() > 0) {
                    try {
                        LogEntry entry = readLogEntry(input);
                        entries.add(entry);
                    } catch (EOFException e) {
                        // End of file reached
                        break;
                    } catch (IOException e) {
                        System.err.println("⚠️  Corrupted WAL entry detected, stopping recovery: " + e.getMessage());
                        break;
                    }
                }
            }

            System.out.println("✅ Recovered " + entries.size() + " operations from WAL");
            return entries;
        } finally {
            lock.readLock().unlock();
        }
    }

    private LogEntry readLogEntry(DataInputStream input) throws IOException {
        byte typeOrdinal = input.readByte();
        LogEntry.LogType type = LogEntry.LogType.values()[typeOrdinal];
        long timestamp = input.readLong();

        int keyLen = input.readInt();
        byte[] keyBytes = new byte[keyLen];
        input.readFully(keyBytes);
        String key = new String(keyBytes, StandardCharsets.UTF_8);

        int valueLen = input.readInt();
        String value = null;
        if (valueLen > 0) {
            byte[] valueBytes = new byte[valueLen];
            input.readFully(valueBytes);
            value = new String(valueBytes, StandardCharsets.UTF_8);
        }

        return new LogEntry(type, key, value, timestamp);
    }

    /**
     * Clears the WAL file after successful flush to SSTable
     */
    public void clear() throws IOException {
        lock.writeLock().lock();
        try {
            if (closed) {
                return;
            }

            // Close current streams
            outputStream.close();
            bufferedStream.close();
            fileStream.close();

            // Delete the WAL file
            Files.deleteIfExists(walPath);

            // Reopen for new writes
            initializeStreams();

            walSequenceNumber = 0;
            System.out.println("🧹 WAL cleared successfully");
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Gets the current WAL sequence number
     */
    public long getSequenceNumber() {
        lock.readLock().lock();
        try {
            return walSequenceNumber;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Checks if WAL is empty
     */
    public boolean isEmpty() throws IOException {
        lock.readLock().lock();
        try {
            return !Files.exists(walPath) || Files.size(walPath) == 0;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Closes the WAL
     */
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (!closed) {
                outputStream.close();
                bufferedStream.close();
                fileStream.close();
                closed = true;
                System.out.println("📚 WAL closed");
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Log entry class for recovery
     */
    public static class LogEntry {
        public enum LogType {
            PUT, DELETE
        }

        private final LogType type;
        private final String key;
        private final String value;
        private final long timestamp;

        public LogEntry(LogType type, String key, String value, long timestamp) {
            this.type = type;
            this.key = key;
            this.value = value;
            this.timestamp = timestamp;
        }

        public LogType getType() { return type; }
        public String getKey() { return key; }
        public String getValue() { return value; }
        public long getTimestamp() { return timestamp; }

        public KeyValuePair toKeyValuePair(int generation) {
            return new KeyValuePair(key, value, type == LogType.DELETE, timestamp, generation);
        }

        @Override
        public String toString() {
            return String.format("LogEntry{type=%s, key='%s', value='%s', timestamp=%d}",
                    type, key, value, timestamp);
        }
    }
}