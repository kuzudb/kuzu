package com.kuzudb.java_test;

import com.kuzudb.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.AfterAll;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

import java.io.IOException;

public class TestBase {

    protected static KuzuDatabase db;
    protected static KuzuConnection conn;
    @TempDir
    static Path tempDir;

    @BeforeAll
    static void getDBandConn() throws IOException, KuzuObjectRefDestroyedException {
        TestHelper.loadData(tempDir.toFile().getAbsolutePath());
        db = TestHelper.getDatabase();
        conn = TestHelper.getConnection();
    }

    @AfterAll
    static void destroyDBandConn() throws KuzuObjectRefDestroyedException {
        try {
            db.destroy();
            conn.destroy();
        } catch (AssertionError e) {
            fail("destroyDBandConn failed: ");
        }
    }

}
