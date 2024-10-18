package com.kuzudb;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

public class DatabaseTest extends TestBase {
    @TempDir
    static Path tempDir;

    @Test
    void DBCreationAndDestroyWithArgs() {
        String dbPath = "";
        try {
            dbPath = tempDir.toFile().getAbsolutePath();
        } catch (Exception e) {
            fail("Cannot get database path: " + e.getMessage());
        }

        try (Database database = new Database(
            dbPath,
            1 << 28 /* 256 MB */,
            true /* compression */,
            false /* readOnly */,
            1 << 30 /* 1 GB */)) {
        // Database will be automatically destroyed after this block
        } catch (Exception e) {
            fail("DBCreationAndDestroyWithArgs failed: " + e.getMessage());
        }
    }

    @Test
    void DBCreationAndDestroyWithPathOnly() {
        String dbPath = "";
        try {
            dbPath = tempDir.toFile().getAbsolutePath();
        } catch (Exception e) {
            fail("Cannot get database path: " + e.getMessage());
        }  
        
        try (Database database = new Database(dbPath)) {
        } catch (Exception e) {
            fail("DBCreationAndDestroyWithPathOnly failed: " + e.getMessage());
        }
    }

    @Test
    void DBCreationAndDestroyWithNoParam(){
        try (Database database = new Database()) {
        } catch (Exception e) {
            fail("DBCreationAndDestroyWithNoParam failed: " + e.getMessage());
        }
    }
}
