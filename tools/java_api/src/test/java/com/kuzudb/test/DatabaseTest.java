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
    void DBCreationAndDestroy() {
        try {
            String dbPath = tempDir.toFile().getAbsolutePath();
            KuzuDatabase database = new KuzuDatabase(dbPath, 1024 * 1024 * 1024, true /* compression */, false /* readOnly */);
            database.destroy();
            database = new KuzuDatabase(dbPath);
            database.destroy();
        } catch (Exception e) {
            fail("DBCreationAndDestroy failed");
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

    @Test
    void DBSetLoggingLevel() {
        try {
            KuzuDatabase.setLoggingLevel("debug");
            KuzuDatabase.setLoggingLevel("info");
            KuzuDatabase.setLoggingLevel("err");
        } catch (Exception e) {
            fail("DBSetLoggingLevel failed: ");
        }

        try {
            KuzuDatabase.setLoggingLevel("unsupported");
            fail("DBSetLoggingLevel did not throw exception as expected.");
        } catch (Exception e) {
        }
    }
}
