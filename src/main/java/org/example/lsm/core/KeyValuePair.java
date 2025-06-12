package org.example.lsm.core;

public class KeyValuePair implements Comparable<KeyValuePair> {
    private final String key;
    private final String value;
    private final boolean deleted;
    private final long timestamp;
    private final int generation;

    public KeyValuePair(String key, String value) {
        this(key, value, false, System.nanoTime(), 0);
    }

    public KeyValuePair(String key, String value, boolean deleted) {
        this(key, value, deleted, System.nanoTime(), 0);
    }

    public KeyValuePair(String key, String value, boolean deleted, long timestamp, int generation) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        this.key = key;
        this.value = value;
        this.deleted = deleted;
        this.timestamp = timestamp;
        this.generation = generation;
    }

    public String getKey() {
        return key;
    }

    public String getValue() {
        return value;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public int getGeneration() {
        return generation;
    }

    public static KeyValuePair tombstone(String key) {
        return new KeyValuePair(key, null, true, System.nanoTime(), 0);
    }

    public static KeyValuePair tombstone(String key, long timestamp, int generation) {
        return new KeyValuePair(key, null, true, timestamp, generation);
    }

    public KeyValuePair withGeneration(int newGeneration) {
        return new KeyValuePair(this.key, this.value, this.deleted, this.timestamp, newGeneration);
    }

    public boolean isNewerThan(KeyValuePair other) {
        if (!this.key.equals(other.key)) {
            throw new IllegalArgumentException("Cannot compare entries with different keys");
        }

        if (this.timestamp != other.timestamp) {
            return this.timestamp > other.timestamp;
        }
        return this.generation > other.generation;
    }

    @Override
    public int compareTo(KeyValuePair other) {
        return this.key.compareTo(other.key);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;

        KeyValuePair other = (KeyValuePair) obj;
        return deleted == other.deleted &&
                timestamp == other.timestamp &&
                generation == other.generation &&
                key.equals(other.key) &&
                java.util.Objects.equals(value, other.value);
    }

    @Override
    public int hashCode() {
        return java.util.Objects.hash(key, value, deleted, timestamp, generation);
    }

    @Override
    public String toString() {
        return String.format("KeyValuePair{key='%s', value='%s', deleted=%s, timestamp=%d, generation=%d}",
                key, value, deleted, timestamp, generation);
    }

    public long estimateSize() {
        int keySize = key.getBytes(java.nio.charset.StandardCharsets.UTF_8).length;
        int valueSize = (value != null) ? value.getBytes(java.nio.charset.StandardCharsets.UTF_8).length : 0;
        return 1 + 4 + keySize + 4 + valueSize + 8 + 4 + 16;
    }
}