package com.kuzudb.java_test;

import com.kuzudb.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

import java.io.IOException;

public class DatabaseTest extends TestBase {
    @TempDir
    static Path tempDir;

    @Test
    void DBCreationAndDestroyWithArgs() {
        try {
            String dbPath = tempDir.toFile().getAbsolutePath();
            KuzuDatabase database = new KuzuDatabase(
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
            KuzuDatabase database = new KuzuDatabase(dbPath);
            database.destroy();
        } catch (Exception e) {
            fail("DBCreationAndDestroyWithPathOnly failed");
        }
    }

    @Test
    void DBInvalidPath() {
        try {
            KuzuDatabase database = new KuzuDatabase("");
            database.destroy();
            fail("DBInvalidPath did not throw exception as expected.");
        } catch (Exception e) {
        }
    }
}
