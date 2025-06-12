package org.example.lsm.core;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;

public class BloomFilter implements Serializable {
    private static final long serialVersionUID = 1L;

    private final BitSet bitSet;
    private final int bitSetSize;
    private final int expectedElements;
    private final int numHashFunctions;
    private int addedElements;

    /**
     * Creates a Bloom filter with the specified expected number of elements and false positive probability
     */
    public BloomFilter(int expectedElements, double falsePositiveProbability) {
        if (expectedElements <= 0) {
            expectedElements = 1;
        }
        if (falsePositiveProbability <= 0.0 || falsePositiveProbability >= 1.0) {
            falsePositiveProbability = 0.01;
        }

        this.expectedElements = expectedElements;
        this.bitSetSize = calculateOptimalBitSetSize(expectedElements, falsePositiveProbability);
        this.numHashFunctions = calculateOptimalHashFunctions(bitSetSize, expectedElements);
        this.bitSet = new BitSet(bitSetSize);
        this.addedElements = 0;
    }

    /**
     * Private constructor for deserialization
     */
    private BloomFilter(BitSet bitSet, int bitSetSize, int expectedElements, int numHashFunctions, int addedElements) {
        this.bitSet = bitSet;
        this.bitSetSize = bitSetSize;
        this.expectedElements = expectedElements;
        this.numHashFunctions = numHashFunctions;
        this.addedElements = addedElements;
    }

    /**
     * Adds an element to the Bloom filter
     */
    public void add(String element) {
        if (element == null) {
            return;
        }

        int[] hashes = getHashes(element);
        for (int hash : hashes) {
            int index = Math.abs(hash % bitSetSize);
            bitSet.set(index);
        }
        addedElements++;
    }

    /**
     * Tests whether an element might be in the set
     */
    public boolean mightContain(String element) {
        if (element == null) {
            return false;
        }

        int[] hashes = getHashes(element);
        for (int hash : hashes) {
            int index = Math.abs(hash % bitSetSize);
            if (!bitSet.get(index)) {
                return false;
            }
        }
        return true;
    }

    /**
     * Generates hash values for an element
     */
    private int[] getHashes(String element) {
        int[] hashes = new int[numHashFunctions];

        try {
            // Use MD5 for hash generation
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            byte[] digest = md5.digest(element.getBytes(StandardCharsets.UTF_8));

            // Convert first 8 bytes to two integers for double hashing
            int hash1 = bytesToInt(digest, 0);
            int hash2 = bytesToInt(digest, 4);

            // Generate multiple hash values using double hashing technique
            for (int i = 0; i < numHashFunctions; i++) {
                hashes[i] = hash1 + (i * hash2);
            }

        } catch (NoSuchAlgorithmException e) {
            // Fallback to simple hash if MD5 is not available
            int baseHash = element.hashCode();
            for (int i = 0; i < numHashFunctions; i++) {
                hashes[i] = baseHash + (i * 31);
            }
        }

        return hashes;
    }

    /**
     * Converts 4 bytes to an integer
     */
    private int bytesToInt(byte[] bytes, int offset) {
        return ((bytes[offset] & 0xFF) << 24) |
                ((bytes[offset + 1] & 0xFF) << 16) |
                ((bytes[offset + 2] & 0xFF) << 8) |
                (bytes[offset + 3] & 0xFF);
    }

    /**
     * Calculates optimal bit set size
     */
    private int calculateOptimalBitSetSize(int expectedElements, double falsePositiveProbability) {
        return (int) Math.ceil(-expectedElements * Math.log(falsePositiveProbability) / (Math.log(2) * Math.log(2)));
    }

    /**
     * Calculates optimal number of hash functions
     */
    private int calculateOptimalHashFunctions(int bitSetSize, int expectedElements) {
        return Math.max(1, (int) Math.round((double) bitSetSize / expectedElements * Math.log(2)));
    }

    /**
     * Gets the current false positive probability
     */
    public double getCurrentFalsePositiveProbability() {
        if (addedElements == 0) {
            return 0.0;
        }

        double ratio = (double) addedElements / expectedElements;
        return Math.pow(1 - Math.exp(-numHashFunctions * ratio), numHashFunctions);
    }

    /**
     * Saves the Bloom filter to a file
     */
    public void writeToFile(String filePath) throws IOException {
        try (ObjectOutputStream out = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(filePath)))) {
            out.writeObject(this);
        }
    }

    /**
     * Loads a Bloom filter from a file
     */
    public static BloomFilter readFromFile(String filePath) throws IOException, ClassNotFoundException {
        try (ObjectInputStream in = new ObjectInputStream(new BufferedInputStream(new FileInputStream(filePath)))) {
            return (BloomFilter) in.readObject();
        }
    }

    /**
     * Custom serialization
     */
    private void writeObject(ObjectOutputStream out) throws IOException {
        out.writeInt(bitSetSize);
        out.writeInt(expectedElements);
        out.writeInt(numHashFunctions);
        out.writeInt(addedElements);

        // Serialize BitSet
        byte[] bitSetBytes = bitSet.toByteArray();
        out.writeInt(bitSetBytes.length);
        out.write(bitSetBytes);
    }

    /**
     * Custom deserialization
     */
    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        int bitSetSize = in.readInt();
        int expectedElements = in.readInt();
        int numHashFunctions = in.readInt();
        int addedElements = in.readInt();

        // Deserialize BitSet
        int bitSetBytesLength = in.readInt();
        byte[] bitSetBytes = new byte[bitSetBytesLength];
        in.readFully(bitSetBytes);
        BitSet bitSet = BitSet.valueOf(bitSetBytes);

        // Set fields using reflection-like approach (since fields are final)
        try {
            java.lang.reflect.Field bitSetField = BloomFilter.class.getDeclaredField("bitSet");
            bitSetField.setAccessible(true);
            bitSetField.set(this, bitSet);

            java.lang.reflect.Field bitSetSizeField = BloomFilter.class.getDeclaredField("bitSetSize");
            bitSetSizeField.setAccessible(true);
            bitSetSizeField.set(this, bitSetSize);

            java.lang.reflect.Field expectedElementsField = BloomFilter.class.getDeclaredField("expectedElements");
            expectedElementsField.setAccessible(true);
            expectedElementsField.set(this, expectedElements);

            java.lang.reflect.Field numHashFunctionsField = BloomFilter.class.getDeclaredField("numHashFunctions");
            numHashFunctionsField.setAccessible(true);
            numHashFunctionsField.set(this, numHashFunctions);

            java.lang.reflect.Field addedElementsField = BloomFilter.class.getDeclaredField("addedElements");
            addedElementsField.setAccessible(true);
            addedElementsField.set(this, addedElements);

        } catch (Exception e) {
            throw new IOException("Failed to deserialize BloomFilter", e);
        }
    }

    // Getters for statistics
    public int getBitSetSize() { return bitSetSize; }
    public int getExpectedElements() { return expectedElements; }
    public int getNumHashFunctions() { return numHashFunctions; }
    public int getAddedElements() { return addedElements; }

    /**
     * Gets statistics about the Bloom filter
     */
    public String getStats() {
        return String.format("BloomFilter{size=%d, elements=%d/%d, hashFuncs=%d, falsePos=%.4f%%}",
                bitSetSize, addedElements, expectedElements, numHashFunctions,
                getCurrentFalsePositiveProbability() * 100);
    }

    @Override
    public String toString() {
        return getStats();
    }
}