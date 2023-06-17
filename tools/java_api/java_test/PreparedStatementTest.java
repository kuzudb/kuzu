package tools.java_api.java_test;

import tools.java_api.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.AfterAll;

public class PreparedStatementTest {
    private static  KuzuDatabase db;
    private static KuzuConnection conn;

    @BeforeAll
    static void getDBandConn() {
        System.out.println("KuzuPreparedStatement test starting, loading data...");
        TestHelper.loadData();
        db = TestHelper.getDatabase();
        conn = TestHelper.getConnection();
        System.out.println("Test data loaded");
    }

    @AfterAll
    static void destroyDBandConn() {
        System.out.println("KuzuPreparedStatement test finished, cleaning up data...");
        try{
            TestHelper.cleanup();
            db.destory();
            conn.destory();
        }catch(AssertionError e) {
            fail("destroyDBandConn failed: ");
            System.out.println(e.toString());
        }
        System.out.println("Data cleaned up");
    }

    @Test 
    void PrepStmtIsSuccess() {
        String query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
        KuzuPreparedStatement preparedStatement1 = conn.prepare(query);
        assertNotNull(preparedStatement1);
        assertTrue(preparedStatement1.isSuccess());
        preparedStatement1.destory();

        query = "MATCH (a:personnnn) WHERE a.isStudent = $1 RETURN COUNT(*)";
        KuzuPreparedStatement preparedStatement2 = conn.prepare(query);
        assertNotNull(preparedStatement2);
        assertFalse(preparedStatement2.isSuccess());
        preparedStatement2.destory();

        System.out.println("PrepStmtIsSuccess passed");
    }

    @Test
    void PrepStmtGetErrorMessage() {
        String query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
        KuzuPreparedStatement preparedStatement1 = conn.prepare(query);
        assertNotNull(preparedStatement1);
        String message = preparedStatement1.getErrorMessage();
        assertTrue(message.equals(""));
        preparedStatement1.destory();

        query = "MATCH (a:personnnn) WHERE a.isStudent = $1 RETURN COUNT(*)";
        KuzuPreparedStatement preparedStatement2 = conn.prepare(query);
        assertNotNull(preparedStatement2);
        message = preparedStatement2.getErrorMessage();
        assertTrue(message.equals("Binder exception: Node table personnnn does not exist."));
        preparedStatement2.destory();

        System.out.println("PrepStmtGetErrorMessage passed");
    }

    @Test
    void PrepStmtAllowActiveTransaction() {
        String query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
        KuzuPreparedStatement preparedStatement1 = conn.prepare(query);
        assertNotNull(preparedStatement1);
        assertTrue(preparedStatement1.isSuccess());
        assertTrue(preparedStatement1.allowActiveTransaction());
        preparedStatement1.destory();

        query = "create node table npytable (id INT64,i64 INT64[12],PRIMARY KEY(id));";
        KuzuPreparedStatement preparedStatement2 = conn.prepare(query);
        assertNotNull(preparedStatement2);
        assertTrue(preparedStatement2.isSuccess());
        assertFalse(preparedStatement2.allowActiveTransaction());
        preparedStatement2.destory();

        System.out.println("PrepStmtAllowActiveTransaction passed");
    }


}
