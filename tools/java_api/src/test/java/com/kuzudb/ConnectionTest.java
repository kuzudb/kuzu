package com.kuzudb;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.MethodOrderer;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;
import java.util.HashMap;
import java.time.LocalDate;
import java.time.Instant;
import java.time.Duration;


@TestMethodOrder(MethodOrderer.MethodName.class)
public class ConnectionTest extends TestBase {

    @Test
    void ConnCreationAndDestroy() {
        try (Connection conn = new Connection(db)) {
        } catch (AssertionError e) {
            fail("ConnCreationAndDestroy failed");
        } catch (RuntimeException e) {
            fail("ConnCreationAndDestroy failed");
        }
    }

    @Test
    void ConnInvalidDB() {
        try {
            Connection conn = new Connection(null);
            fail("DBInvalidPath did not throw RuntimeException as expected.");
        } catch (AssertionError e) {
        }
    }

    @Test
    void ConnQuery() {
        String query = "MATCH (a:person) RETURN a.fName;";
        try (QueryResult result = conn.query(query)) {
            assertNotNull(result);
            assertTrue(result.isSuccess());
            assertTrue(result.hasNext());
            assertTrue(result.getErrorMessage().equals(""));
            assertEquals(result.getNumTuples(), 8);
            assertEquals(result.getNumColumns(), 1);
            assertTrue(result.getColumnName(0).equals("a.fName"));
        }
    }

    @Test
    void ConnSetGetMaxNumThreadForExec() {
        conn.setMaxNumThreadForExec(4);
        assertEquals(conn.getMaxNumThreadForExec(), 4);
        conn.setMaxNumThreadForExec(8);
        assertEquals(conn.getMaxNumThreadForExec(), 8);
    }

    @Test
    void ConnPrepareBool() {
        String query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
        Map<String, Value> m = new HashMap<String, Value>();
        m.put("1", new Value(true));
        PreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        QueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        FlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 3);
        statement.close();
        result.close();
    }

    @Test
    void ConnPrepareInt64() {
        String query = "MATCH (a:person) WHERE a.age > $1 RETURN COUNT(*)";
        Map<String, Value> m = new HashMap<String, Value>();

        m.put("1", new Value((long) 30));

        PreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        QueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        FlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 4);
        statement.close();
        result.close();
    }

    @Test
    void ConnPrepareInt32() {
        String query = "MATCH (a:movies) WHERE a.length > $1 RETURN COUNT(*)";
        Map<String, Value> m = new HashMap<String, Value>();
        m.put("1", new Value((int) 200));
        PreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        QueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        FlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 2);
        statement.close();
        result.close();
    }

    @Test
    void ConnPrepareInt16() {
        String query = "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.length > $1 RETURN COUNT(*)";
        Map<String, Value> m = new HashMap<String, Value>();
        m.put("1", new Value((short) 10));
        PreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        QueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        FlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 2);
        statement.close();
        result.close();
    }

    @Test
    void ConnPrepareDouble() {
        String query = "MATCH (a:person) WHERE a.eyeSight > $1 RETURN COUNT(*)";
        Map<String, Value> m = new HashMap<String, Value>();
        m.put("1", new Value((double) 4.5));
        PreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        QueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        FlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 7);
        statement.close();
        result.close();
    }

    @Test
    void ConnPrepareFloat() {
        String query = "MATCH (a:person) WHERE a.height < $1 RETURN COUNT(*)";
        Map<String, Value> m = new HashMap<String, Value>();
        m.put("1", new Value((float) 1.0));
        PreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        QueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        FlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 1);
        statement.close();
        result.close();
    }

    @Test
    void ConnPrepareString() {
        String query = "MATCH (a:person) WHERE a.fName = $1 RETURN COUNT(*)";
        Map<String, Value> m = new HashMap<String, Value>();
        m.put("1", new Value("Alice"));
        PreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        QueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        FlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 1);
        statement.close();
        result.close();
    }

    @Test
    void ConnPrepareDate() {
        String query = "MATCH (a:person) WHERE a.birthdate > $1 RETURN COUNT(*)";
        Map<String, Value> m = new HashMap<String, Value>();
        m.put("1", new Value(LocalDate.ofEpochDay(0)));
        PreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        QueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        FlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 4);
        statement.close();
        result.close();
    }

    @Test
    void ConnPrepareTimeStamp() {
        String query = "MATCH (a:person) WHERE a.registerTime > $1 RETURN COUNT(*)";
        Map<String, Value> m = new HashMap<String, Value>();
        m.put("1", new Value(Instant.EPOCH));
        PreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        QueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        FlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 7);
        statement.close();
        result.close();
    }

    @Test
    void ConnPrepareInterval() {
        String query = "MATCH (a:person) WHERE a.lastJobDuration > $1 RETURN COUNT(*)";
        Map<String, Value> m = new HashMap<String, Value>();
        m.put("1", new Value(Duration.ofDays(3650)));
        PreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        QueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        FlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 3);
        statement.close();
        result.close();
    }

    @Test
    void ConnPrepareMultiParam() {
        String query = "MATCH (a:person) WHERE a.lastJobDuration > $1 AND a.fName = $2 RETURN COUNT(*)";
        Map<String, Value> m = new HashMap<String, Value>();
        Value v1 = new Value(Duration.ofDays(730));
        Value v2 = new Value("Alice");
        m.put("1", v1);
        m.put("2", v2);
        PreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        QueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        FlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 1);
        statement.close();
        result.close();

        // Not strictly necessary, but this makes sure if we freed v1 or v2 in
        // the execute() call, we segfault here.
        v1.close();
        v2.close();
    }

    @Test
    void ConnPrepareLimit() {
        String query = "MATCH (a:person) RETURN a.ID LIMIT $lt";
        Map<String, Value> m = new HashMap<String, Value>();
        m.put("lt", new Value((int) 2));
        PreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        QueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 2);
        assertEquals(result.getNumColumns(), 1);
        FlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 0);
        tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 2);
        statement.close();
        result.close();
    }

    @Test
    void ConnQueryTimeout() {
        conn.setQueryTimeout(1);
        try (QueryResult result = conn.query("UNWIND RANGE(1,100000) AS x UNWIND RANGE(1, 100000) AS y RETURN COUNT(x + y);")) {
            assertNotNull(result);
            assertFalse(result.isSuccess());
            assertTrue(result.getErrorMessage().equals("Interrupted."));
        }
    }

}
