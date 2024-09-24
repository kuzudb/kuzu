package com.kuzudb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class FlatTupleTest extends TestBase {

    @Test
    void FlatTupleGetValue() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age, a.height ORDER BY a.fName LIMIT 1");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        assertNotNull(flatTuple);

        Value value = flatTuple.getValue(0);
        assertNotNull(value);
        assertEquals(value.getDataType().getID(), DataTypeID.STRING);
        assertTrue(value.getValue().equals("Alice"));
        value.destroy();

        value = flatTuple.getValue(1);
        assertNotNull(value);
        assertEquals(value.getDataType().getID(), DataTypeID.INT64);
        assertTrue(value.getValue().equals(35L));
        value.destroy();

        value = flatTuple.getValue(2);
        assertNotNull(value);
        assertEquals(value.getDataType().getID(), DataTypeID.FLOAT);
        assertTrue(value.getValue().equals((float) 1.731));
        value.destroy();

        value = flatTuple.getValue(222);
        assertNull(value);

        result.destroy();
    }

    @Test
    void FlatTupleToString() throws ObjectRefDestroyedException {
        QueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age, a.height ORDER BY a.fName LIMIT 1");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());
        FlatTuple flatTuple = result.getNext();
        assertNotNull(flatTuple);

        String str = flatTuple.toString();
        assertTrue(str.equals("Alice|35|1.731000\n"));

        flatTuple.destroy();
        result.destroy();

    }

}
