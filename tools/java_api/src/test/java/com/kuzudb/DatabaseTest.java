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
        try {
            String dbPath = tempDir.toFile().getAbsolutePath();
            Database database = new Database(
                    dbPath,
                    1 << 28 /* 256 MB */,
                    true /* compression */,
                    false /* readOnly */,
                    1 << 30 /* 1 GB */
            );
            database.destroy();
        } catch (Exception e) {
            fail("DBCreationAndDestroyWithArgs failed");
        }
    }

    @Test
    void DBCreationAndDestroyWithPathOnly() {
        try {
            String dbPath = tempDir.toFile().getAbsolutePath();
            Database database = new Database(dbPath);
            database.destroy();
        } catch (Exception e) {
            fail("DBCreationAndDestroyWithPathOnly failed");
        }
    }

    @Test
    void DBCreationAndDestroyWithNoParam(){
        try {
            Database database = new Database();
            database.destroy();
        } catch (Exception e) {
            fail("DBCreationAndDestroyWithNoParam failed");
        }
    }
}
