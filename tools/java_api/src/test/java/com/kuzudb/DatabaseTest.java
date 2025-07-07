package com.kuzudb;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

public class DatabaseTest extends TestBase {
    @Test
    void DBCreationAndDestroyWithArgs() {
        String dbPath = "";
        try {
            dbPath = tempDir.resolve("db1.kz").toString();
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
    void DBCreationWithInvalidMaxDBSize() {
        String dbPath = "";
        try {
            dbPath = tempDir.resolve("db2.kz").toString();
        } catch (Exception e) {
            fail("Cannot get database path: " + e.getMessage());
        }
            try {
                Database database = new Database(
                            dbPath,
                            1 << 28 /* 256 MB */,
                            true /* compression */,
                            false /* readOnly */,
                            (1 << 30) - 1 /* 1 GB - 1 Byte (Odd Number)*/,
                            false /* autoCheckpoint */,
                            1234 /* checkpointThreshold */);

            fail("DBCreationWithInvalidMaxDBSize failed:");
        }
        catch (Exception e) {
            assertEquals("Buffer manager exception: The given max db size should be a power of 2.", e.getMessage());
            return;
        }
        fail("DBCreationWithInvalidMaxDBSize failed:");
    }


    @Test
    void DBCreationAndDestroyWithPathOnly() {
        String dbPath = "";
        try {
            dbPath = tempDir.resolve("db3.kz").toString();
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

    @Test
    void DBDestroyBeforeConnectionAndQueryResult(){
        Database database = new Database(":memory:", 1 << 28, true, false, 1 << 30, false, 0);
        Connection conn = new Connection(database);
        QueryResult result = conn.query("RETURN 1");
        assertTrue(result.hasNext());
        assertEquals(result.getNext().getValue(0).toString(), "1");
        database.close();
        try {
            result.getNext();
            fail("DBDestroyBeforeConnectionAndQueryResult failed: QueryResult should not be usable after database is closed.");
        } catch (Exception e) {
            assertEquals("Runtime exception: The current operation is not allowed because the parent database is closed.", e.getMessage());
        }
        try {
            conn.query("RETURN 1");
            fail("DBDestroyBeforeConnectionAndQueryResult failed: Connection should not be usable after database is closed.");
        } catch (Exception e) {
            assertEquals("Runtime exception: The current operation is not allowed because the parent database is closed.", e.getMessage());
        }
        result.close();
        conn.close();
    }
}
