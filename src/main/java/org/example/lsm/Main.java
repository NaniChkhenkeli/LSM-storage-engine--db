package org.example.lsm;

import org.example.lsm.core.LSMDatabase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.Scanner;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Main {
    private static final String DATA_DIRECTORY = "lsm_data"; // Directory to store database files
    private static LSMDatabase database;
    private static ScheduledExecutorService printScheduler;

    public static void main(String[] args) {
        // Clear previous data for a fresh start (useful for development)
        clearDataDirectory(DATA_DIRECTORY);

        try {
            database = new LSMDatabase(DATA_DIRECTORY);
            System.out.println("\n🎉 LSM-Tree Database initialized successfully in '" + DATA_DIRECTORY + "'\n");

            // Schedule a periodic print of status (optional, for demo visibility)
            printScheduler = Executors.newSingleThreadScheduledExecutor();
            printScheduler.scheduleAtFixedRate(database::printStatus, 10, 30, TimeUnit.SECONDS);

            startConsole();
        } catch (IOException e) {
            System.err.println("Fatal error initializing database: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        } finally {
            if (database != null) {
                try {
                    // Perform final flush and shutdown
                    database.close();
                } catch (Exception e) {
                    System.err.println("Error during database shutdown: " + e.getMessage());
                    e.printStackTrace();
                }
            }
            if (printScheduler != null) {
                printScheduler.shutdown();
                try {
                    if (!printScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                        printScheduler.shutdownNow();
                    }
                } catch (InterruptedException e) {
                    printScheduler.shutdownNow();
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private static void clearDataDirectory(String dirName) {
        System.out.println("🗑️  Clearing previous data in '" + dirName + "'...");
        Path dirPath = Paths.get(dirName);
        if (Files.exists(dirPath)) {
            try (var stream = Files.walk(dirPath)) {
                stream.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(java.io.File::delete);
            } catch (IOException e) {
                System.err.println("Error clearing data directory: " + e.getMessage());
            }
        }
        System.out.println("✅ Data directory cleared.");
    }

    private static void startConsole() {
        Scanner scanner = new Scanner(System.in);
        displayHelp();

        while (true) {
            System.out.print("\nLSM-Tree> ");
            String line = scanner.nextLine().trim();
            if (line.isEmpty()) {
                continue;
            }

            String[] parts = line.split("\\s+", 3); // Split into max 3 parts (command, key, value)
            String command = parts[0].toLowerCase();

            try {
                switch (command) {
                    case "s":
                    case "set":
                        if (parts.length == 3) {
                            String key = parts[1];
                            String value = parts[2];
                            database.put(key, value);
                        } else {
                            System.out.println("Usage: set <key> <value>");
                        }
                        break;
                    case "r":
                    case "rgn":
                        if (parts.length == 3) {
                            try {
                                int start = Integer.parseInt(parts[1]);
                                int end = Integer.parseInt(parts[2]);
                                if (start > end) {
                                    System.out.println("Error: Start key must be less than or equal to end key.");
                                } else {
                                    database.insertRange(start, end);
                                }
                            } catch (NumberFormatException e) {
                                System.out.println("Error: Start and end must be numbers. Usage: rgn <start> <end>");
                            }
                        } else {
                            System.out.println("Usage: rgn <start> <end>");
                        }
                        break;
                    case "g":
                    case "get":
                        if (parts.length == 2) {
                            String key = parts[1];
                            String value = database.get(key);
                            if (value != null) {
                                System.out.println("🔑 Found: " + key + " = " + value);
                            } else {
                                System.out.println("❌ Key '" + key + "' not found or deleted.");
                            }
                        } else {
                            System.out.println("Usage: get <key>");
                        }
                        break;
                    case "d":
                    case "del":
                        if (parts.length == 2) {
                            String key = parts[1];
                            database.delete(key);
                            System.out.println("🗑️  Tombstone placed for key: " + key);
                        } else {
                            System.out.println("Usage: del <key>");
                        }
                        break;
                    case "p":
                    case "prt":
                        database.printStatus();
                        break;
                    case "e":
                    case "exit":
                        System.out.println("👋 Exiting LSM-Tree console. Goodbye!");
                        scanner.close();
                        return; // Exit the loop and main method
                    case "h":
                    case "help":
                        displayHelp();
                        break;
                    default:
                        System.out.println("Unknown command. Type 'help' for available commands.");
                        break;
                }
            } catch (IOException e) {
                System.err.println("An I/O error occurred: " + e.getMessage());
                e.printStackTrace();
            } catch (Exception e) {
                System.err.println("An unexpected error occurred: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private static void displayHelp() {
        System.out.println("\nCommands:");
        System.out.println("  - s/set   <key> <value> : insert a key-value pair");
        System.out.println("  - r/rgn   <start> <end> : insert this range of numeric keys with random values");
        System.out.println("  - g/get   <key>         : get a key value");
        System.out.println("  - d/del   <key>         : delete a key");
        System.out.println("  - p/prt                 : print current tree status");
        System.out.println("  - e/exit                : stop the console");
        System.out.println("  - h/help                : show this message");
    }
}