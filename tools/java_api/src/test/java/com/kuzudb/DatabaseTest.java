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
                1234 /* checkpointThreshold */,
                true /* throwOnWalReplayFailure */,
                true /* enableChecksums */)) {
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
                            1234 /* checkpointThreshold */,
                            true /* throwOnWalReplayFailure */,
                            true /* enableChecksums */);

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
        Database database = new Database(":memory:", 1 << 28, true, false, 1 << 30, false, 0, true, true);
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

    @Test
    void DBCreationTestThrowOnWALReplayFailure() {
        String dbPath = "";
        try {
            dbPath = tempDir.resolve("db4.kz").toString();
        } catch (Exception e) {
            fail("Cannot get database path: " + e.getMessage());
        }

        // Create a DB, don't checkpoint and leave a WAL
        try (Database database = new Database(
                dbPath)) {
            Connection conn = new Connection(database);
            {
                conn.query("call force_checkpoint_on_close=false");
                conn.query("call auto_checkpoint=false");
                QueryResult result = conn.query("create node table testtest1(id int64 primary key)");
                assertTrue(result.isSuccess());
                result = conn.query("call show_tables() where prefix(name, 'testtest') return name");
                assertEquals(result.getNext().getValue(0).toString(), "testtest1");

                // Trigger an exception, this will also trigger during replay
                result = conn.query("create node table testtest1(id int64 primary key)");
                assertFalse(result.isSuccess());
            }
            conn.close();
            // Database will be automatically destroyed after this block
        } catch (Exception e) {
            fail("DBCreationTestThrowOnWALReplayFailure failed: " + e.getMessage());
        }


        try (Database database = new Database(
                dbPath,
                1 << 28 /* 256 MB */,
                true /* compression */,
                false /* readOnly */,
                1 << 30 /* 1 GB */,
                false /* autoCheckpoint */,
                1234 /* checkpointThreshold */,
                false /* throwOnWalReplayFailure */,
                true /* enableChecksums */)) {
            // Database will be automatically destroyed after this block
            Connection conn = new Connection(database);
            QueryResult result = conn.query("call show_tables() where prefix(name, 'testtest') return name");
            assertTrue(result.isSuccess());
            assertTrue(result.hasNext());
            assertEquals(result.getNext().getValue(0).toString(), "testtest1");
        } catch (Exception e) {
            fail("DBCreationTestThrowOnWALReplayFailure failed: " + e.getMessage());
        }
    }

    @Test
    void DBCreationTestEnableChecksums() {
        String dbPath = "";
        try {
            dbPath = tempDir.resolve("db5.kz").toString();
        } catch (Exception e) {
            fail("Cannot get database path: " + e.getMessage());
        }

        // Create a DB, don't checkpoint and leave a WAL
        try (Database database = new Database(
                dbPath,
                1 << 28 /* 256 MB */,
                true /* compression */,
                false /* readOnly */,
                1 << 30 /* 1 GB */,
                false /* autoCheckpoint */,
                1234 /* checkpointThreshold */,
                true /* throwOnWalReplayFailure */,
                true /* enableChecksums */)) {
            Connection conn = new Connection(database);
            {
                conn.query("call force_checkpoint_on_close=false");
                conn.query("call auto_checkpoint=false");
                QueryResult result = conn.query("create node table testtest1(id int64 primary key)");
                assertTrue(result.isSuccess());
            }
            conn.close();
            // Database will be automatically destroyed after this block
        } catch (Exception e) {
            fail("DBCreationTestEnableChecksums failed: " + e.getMessage());
        }


        try (Database database = new Database(
                dbPath,
                1 << 28 /* 256 MB */,
                true /* compression */,
                false /* readOnly */,
                1 << 30 /* 1 GB */,
                false /* autoCheckpoint */,
                1234 /* checkpointThreshold */,
                true /* throwOnWalReplayFailure */,
                false /* enableChecksums */)) {
            // The database creation should fail
            assertTrue(false);
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("Please open your database using the correct enableChecksums config."));
        }
    }
}
