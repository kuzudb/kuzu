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
            KuzuDatabase database = new KuzuDatabase(dbPath, 0);
            database.destroy();
        } catch (Exception e) {
            fail("DBCreationAndDestroy failed: ");
            System.out.println(e.toString());
        }

        System.out.println("DBCreationAndDestroy passed");
    }

    @Test
    void DBInvalidPath() {
        try {
            KuzuDatabase database = new KuzuDatabase("", 0);
            database.destroy();
            fail("DBInvalidPath did not throw exception as expected.");
        } catch (Exception e) {
            System.out.println("DBInvalidPath passed");
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
            System.out.println(e.toString());
        }

        try {
            KuzuDatabase.setLoggingLevel("unsupported");
            fail("DBSetLoggingLevel did not throw exception as expected.");
        } catch (Exception e) {
        }

        System.out.println("DBSetLoggingLevel passed");
    }
}
