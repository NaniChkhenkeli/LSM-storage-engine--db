package org.example.lsm.util;

import java.io.IOException;
import java.nio.file.*;

public class FileUtils {
    public static void ensureDirectoryExists(String dirPath) throws IOException {
        Path path = Paths.get(dirPath);
        if (!Files.exists(path)) {
            Files.createDirectories(path);
        }
    }

    public static void deleteIfExists(String filePath) throws IOException {
        Path path = Paths.get(filePath);
        Files.deleteIfExists(path);
    }

    public static void atomicMove(String source, String target) throws IOException {
        Files.move(Paths.get(source), Paths.get(target), StandardCopyOption.ATOMIC_MOVE);
    }
}