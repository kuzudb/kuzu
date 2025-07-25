package com.kuzudb;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.AfterAll;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

import java.io.IOException;

public class TestBase {

    protected static Database db;
    protected static Connection conn;
    @TempDir
    static Path tempDir;

    @BeforeAll
    static void getDBandConn() throws IOException {
        String dbPath = tempDir.resolve("db.kz").toString();
        TestHelper.loadData(dbPath);
        db = TestHelper.getDatabase();
        conn = TestHelper.getConnection();
    }

    @AfterAll
    static void destroyDBandConn() {
        try {
            db.close();
            conn.close();
        } catch (AssertionError e) {
            fail("destroyDBandConn failed: ");
        }
    }

}
