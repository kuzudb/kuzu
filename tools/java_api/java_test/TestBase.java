package tools.java_api.java_test;

import tools.java_api.*;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.AfterAll;
import static org.junit.jupiter.api.Assertions.*;
import java.nio.file.Path;

import java.io.IOException;

public class TestBase {

    @TempDir
    static Path tempDir;
    protected static  KuzuDatabase db;
    protected static KuzuConnection conn;

    @BeforeAll
    static void getDBandConn() throws IOException, KuzuObjectRefDestroyedException{
        System.out.println("Kuzu test starting, loading data...");
        TestHelper.loadData(tempDir.toFile().getAbsolutePath());
        db = TestHelper.getDatabase();
        conn = TestHelper.getConnection();
        System.out.println("Test data loaded");
    }

    @AfterAll
    static void destroyDBandConn() throws KuzuObjectRefDestroyedException {
        System.out.println("Kuzu test finished, cleaning up data...");
        try{
            db.destroy();
            conn.destroy();
        }catch(AssertionError e) {
            fail("destroyDBandConn failed: ");
            System.out.println(e.toString());
        }
        System.out.println("Data cleaned up");
    }
    
}
