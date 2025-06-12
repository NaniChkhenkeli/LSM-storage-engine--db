package org.example;

import org.example.lsm.core.LSMDatabase;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;


public class LSMDatabaseTest {

    @Test
    public void testBasicOperations() throws Exception {
        String tempDir = System.getProperty("java.io.tmpdir") + "/lsm_test";

        try (LSMDatabase db = new LSMDatabase(tempDir)) {
            db.put("key1", "value1");
            assertEquals("value1", db.get("key1"));

            db.put("key1", "value2");
            assertEquals("value2", db.get("key1"));

            db.delete("key1");
            assertNull(db.get("key1"));

            db.put("a", "1");
            db.put("b", "2");
            db.put("c", "3");
            assertEquals("1", db.get("a"));
            assertEquals("2", db.get("b"));
            assertEquals("3", db.get("c"));
        }
    }
}