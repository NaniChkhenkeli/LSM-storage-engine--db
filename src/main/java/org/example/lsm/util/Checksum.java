package org.example.lsm.util;

import java.util.zip.CRC32;

public class Checksum {
    public static long calculate(byte[] data) {
        CRC32 crc = new CRC32();
        crc.update(data);
        return crc.getValue();
    }

    public static boolean verify(byte[] data, long checksum) {
        return calculate(data) == checksum;
    }
}