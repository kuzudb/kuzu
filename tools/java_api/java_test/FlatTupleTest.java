package tools.java_api.java_test;

import tools.java_api.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import static org.junit.jupiter.api.Assertions.*;
import org.junit.jupiter.api.AfterAll;

public class FlatTupleTest {
    private static  KuzuDatabase db;
    private static KuzuConnection conn;

    @BeforeAll
    static void getDBandConn() {
        System.out.println("KuzuFlatTuple test starting, loading data...");
        TestHelper.loadData();
        db = TestHelper.getDatabase();
        conn = TestHelper.getConnection();
        System.out.println("Test data loaded");
    }

    @AfterAll
    static void destroyDBandConn() {
        System.out.println("KuzuFlatTuple test finished, cleaning up data...");
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
    void FlatTupleGetValue() {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age, a.height ORDER BY a.fName LIMIT 1");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        assertNotNull(flatTuple);

        KuzuValue value = flatTuple.getValue(0);
        assertNotNull(value);
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.STRING);
        assertTrue(value.getValue().equals("Alice"));
        value.destroy();

        value = flatTuple.getValue(1);
        assertNotNull(value);
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.INT64);
        assertTrue(value.getValue().equals(35L));
        value.destroy();

        value = flatTuple.getValue(2);
        assertNotNull(value);
        assertEquals(value.getDataType().getID(), KuzuDataTypeID.FLOAT);
        assertTrue(value.getValue().equals((float)1.731));
        value.destroy();

        value = flatTuple.getValue(222);
        assertNull(value);
        
        result.destory();
        System.out.println("FlatTupleGetValue passed");
    }

    @Test
    void FlatTupleToString() {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age, a.height ORDER BY a.fName LIMIT 1");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        KuzuFlatTuple flatTuple = result.getNext();
        assertNotNull(flatTuple);
        
        String str = flatTuple.toString();
        assertTrue(str.equals("Alice|35|1.731000\n"));

        flatTuple.destroy();
        result.destory();

        System.out.println("FlatTupleToString passed");
    }

}
