package com.kuzudb;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

public class DatabaseTest extends TestBase {
    @TempDir
    static Path tmpDir;

    @Test
    void DBCreationAndDestroyWithArgs() {
        String dbPath = "";
        try {
            dbPath = tmpDir.toFile().getAbsolutePath();
        } catch (Exception e) {
            fail("Cannot get database path: " + e.getMessage());
        }

        try (Database database = new Database(
                dbPath,
                1 << 28 /* 256 MB */,
                true /* compression */,
                false /* readOnly */,
                1 << 30 /* 1 GB */,
                false /* autoCheckpoint */,
                1234 /* checkpointThreshold */)) {
            Connection conn = new Connection(database);
            {
                QueryResult result = conn.query("CALL current_setting('auto_checkpoint') RETURN *");
                assertEquals(result.getNext().getValue(0).toString(), "False");
            }
            {
                QueryResult result = conn.query("CALL current_setting('checkpoint_threshold') RETURN *");
                assertEquals(result.getNext().getValue(0).toString(), "1234");
            }
            conn.close();
            // Database will be automatically destroyed after this block
        } catch (Exception e) {
            fail("DBCreationAndDestroyWithArgs failed: " + e.getMessage());
        }
    }

    @Test
    void DBCreationAndDestroyWithPathOnly() {
        String dbPath = "";
        try {
            dbPath = tmpDir.toFile().getAbsolutePath();
        } catch (Exception e) {
            fail("Cannot get database path: " + e.getMessage());
        }

        try (Database database = new Database(dbPath)) {
            // check default config
            QueryResult result = conn.query("CALL current_setting('checkpoint_threshold') RETURN *");
            String checkpointThresholdStr = result.getNext().getValue(0).toString();
            assertTrue(Long.parseLong(checkpointThresholdStr) > 0);
        } catch (Exception e) {
            fail("DBCreationAndDestroyWithPathOnly failed: " + e.getMessage());
        }
    }

    @Test
    void DBCreationAndDestroyWithNoParam() {
        try (Database database = new Database()) {
        } catch (Exception e) {
            fail("DBCreationAndDestroyWithNoParam failed: " + e.getMessage());
        }
    }
}
