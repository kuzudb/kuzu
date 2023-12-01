package com.kuzudb.java_test;

import com.kuzudb.*;
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
        try {
            KuzuConnection conn = new KuzuConnection(db);
            conn.destroy();
        } catch (AssertionError e) {
            fail("ConnCreationAndDestroy failed");
        } catch (KuzuObjectRefDestroyedException e) {
            fail("ConnCreationAndDestroy failed");
        }
    }

    @Test
    void ConnInvalidDB() {
        try {
            KuzuConnection conn = new KuzuConnection(null);
            fail("DBInvalidPath did not throw KuzuObjectRefDestroyedException as expected.");
        } catch (AssertionError e) {
        }
    }

    @Test
    void ConnQuery() throws KuzuObjectRefDestroyedException {
        String query = "MATCH (a:person) RETURN a.fName;";
        KuzuQueryResult result = conn.query(query);
        assertNotNull(result);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 8);
        assertEquals(result.getNumColumns(), 1);
        assertTrue(result.getColumnName(0).equals("a.fName"));
        result.destroy();
    }

    @Test
    void ConnSetGetMaxNumThreadForExec() throws KuzuObjectRefDestroyedException {
        conn.setMaxNumThreadForExec(4);
        assertEquals(conn.getMaxNumThreadForExec(), 4);
        conn.setMaxNumThreadForExec(8);
        assertEquals(conn.getMaxNumThreadForExec(), 8);
    }

    @Test
    void ConnPrepareBool() throws KuzuObjectRefDestroyedException {
        String query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
        Map<String, KuzuValue> m = new HashMap<String, KuzuValue>();
        m.put("1", new KuzuValue(true));
        KuzuPreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        KuzuQueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        KuzuFlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 3);
        statement.destroy();
        result.destroy();
    }

    @Test
    void ConnPrepareInt64() throws KuzuObjectRefDestroyedException {
        String query = "MATCH (a:person) WHERE a.age > $1 RETURN COUNT(*)";
        Map<String, KuzuValue> m = new HashMap<String, KuzuValue>();

        m.put("1", new KuzuValue((long) 30));

        KuzuPreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        KuzuQueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        KuzuFlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 4);
        statement.destroy();
        result.destroy();
    }

    @Test
    void ConnPrepareInt32() throws KuzuObjectRefDestroyedException {
        String query = "MATCH (a:movies) WHERE a.length > $1 RETURN COUNT(*)";
        Map<String, KuzuValue> m = new HashMap<String, KuzuValue>();
        m.put("1", new KuzuValue((int) 200));
        KuzuPreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        KuzuQueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        KuzuFlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 2);
        statement.destroy();
        result.destroy();
    }

    @Test
    void ConnPrepareInt16() throws KuzuObjectRefDestroyedException {
        String query = "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.length > $1 RETURN COUNT(*)";
        Map<String, KuzuValue> m = new HashMap<String, KuzuValue>();
        m.put("1", new KuzuValue((short) 10));
        KuzuPreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        KuzuQueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        KuzuFlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 2);
        statement.destroy();
        result.destroy();
    }

    @Test
    void ConnPrepareDouble() throws KuzuObjectRefDestroyedException {
        String query = "MATCH (a:person) WHERE a.eyeSight > $1 RETURN COUNT(*)";
        Map<String, KuzuValue> m = new HashMap<String, KuzuValue>();
        m.put("1", new KuzuValue((double) 4.5));
        KuzuPreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        KuzuQueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        KuzuFlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 7);
        statement.destroy();
        result.destroy();
    }

    @Test
    void ConnPrepareFloat() throws KuzuObjectRefDestroyedException {
        String query = "MATCH (a:person) WHERE a.height < $1 RETURN COUNT(*)";
        Map<String, KuzuValue> m = new HashMap<String, KuzuValue>();
        m.put("1", new KuzuValue((float) 1.0));
        KuzuPreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        KuzuQueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        KuzuFlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 1);
        statement.destroy();
        result.destroy();
    }

    @Test
    void ConnPrepareString() throws KuzuObjectRefDestroyedException {
        String query = "MATCH (a:person) WHERE a.fName = $1 RETURN COUNT(*)";
        Map<String, KuzuValue> m = new HashMap<String, KuzuValue>();
        m.put("1", new KuzuValue("Alice"));
        KuzuPreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        KuzuQueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        KuzuFlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 1);
        statement.destroy();
        result.destroy();
    }

    @Test
    void ConnPrepareDate() throws KuzuObjectRefDestroyedException {
        String query = "MATCH (a:person) WHERE a.birthdate > $1 RETURN COUNT(*)";
        Map<String, KuzuValue> m = new HashMap<String, KuzuValue>();
        m.put("1", new KuzuValue(LocalDate.ofEpochDay(0)));
        KuzuPreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        KuzuQueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        KuzuFlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 4);
        statement.destroy();
        result.destroy();
    }

    @Test
    void ConnPrepareTimeStamp() throws KuzuObjectRefDestroyedException {
        String query = "MATCH (a:person) WHERE a.registerTime > $1 RETURN COUNT(*)";
        Map<String, KuzuValue> m = new HashMap<String, KuzuValue>();
        m.put("1", new KuzuValue(Instant.EPOCH));
        KuzuPreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        KuzuQueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        KuzuFlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 7);
        statement.destroy();
        result.destroy();
    }

    @Test
    void ConnPrepareInterval() throws KuzuObjectRefDestroyedException {
        String query = "MATCH (a:person) WHERE a.lastJobDuration > $1 RETURN COUNT(*)";
        Map<String, KuzuValue> m = new HashMap<String, KuzuValue>();
        m.put("1", new KuzuValue(Duration.ofDays(3650)));
        KuzuPreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        KuzuQueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        KuzuFlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 3);
        statement.destroy();
        result.destroy();
    }

    @Test
    void ConnPrepareMultiParam() throws KuzuObjectRefDestroyedException {
        String query = "MATCH (a:person) WHERE a.lastJobDuration > $1 AND a.fName = $2 RETURN COUNT(*)";
        Map<String, KuzuValue> m = new HashMap<String, KuzuValue>();
        KuzuValue v1 = new KuzuValue(Duration.ofDays(730));
        KuzuValue v2 = new KuzuValue("Alice");
        m.put("1", v1);
        m.put("2", v2);
        KuzuPreparedStatement statement = conn.prepare(query);
        assertNotNull(statement);
        KuzuQueryResult result = conn.execute(statement, m);
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        assertTrue(result.getErrorMessage().equals(""));
        assertEquals(result.getNumTuples(), 1);
        assertEquals(result.getNumColumns(), 1);
        KuzuFlatTuple tuple = result.getNext();
        assertEquals(((long) tuple.getValue(0).getValue()), 1);
        statement.destroy();
        result.destroy();

        // Not strictly necessary, but this makes sure if we freed v1 or v2 in
        // the execute() call, we segfault here.
        v1.destroy();
        v2.destroy();
    }

    @Test
    void ConnQueryTimeout() throws KuzuObjectRefDestroyedException {
        conn.setQueryTimeout(1);
        KuzuQueryResult result = conn.query("MATCH (a:person)-[:knows*1..28]->(b:person) RETURN COUNT(*);");
        assertNotNull(result);
        assertFalse(result.isSuccess());
        assertTrue(result.getErrorMessage().equals("Interrupted."));
        result.destroy();
    }

}
