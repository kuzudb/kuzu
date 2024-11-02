package com.kuzudb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class FlatTupleTest extends TestBase {

    @Test
    void FlatTupleGetValue() throws ObjectRefDestroyedException {
        try (QueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age, a.height ORDER BY a.fName LIMIT 1")) {
            assertTrue(result.isSuccess());
            assertTrue(result.hasNext());

            FlatTuple flatTuple = result.getNext();
            assertNotNull(flatTuple);

            try (Value value = flatTuple.getValue(0)) {
                assertNotNull(value);
                assertEquals(value.getDataType().getID(), DataTypeID.STRING);
                assertTrue(value.getValue().equals("Alice"));
            }

            try (Value value = flatTuple.getValue(1)) {
                assertNotNull(value);
                assertEquals(value.getDataType().getID(), DataTypeID.INT64);
                assertTrue(value.getValue().equals(35L));
            }

            try (Value value = flatTuple.getValue(2)) {
                assertNotNull(value);
                assertEquals(value.getDataType().getID(), DataTypeID.FLOAT);
                assertTrue(value.getValue().equals((float) 1.731));
            }

            Value value = flatTuple.getValue(222);
            assertNull(value);
        }
    }

    @Test
    void FlatTupleToString() throws ObjectRefDestroyedException {
        try (QueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age, a.height ORDER BY a.fName LIMIT 1")) {
            assertTrue(result.isSuccess());
            assertTrue(result.hasNext());

            try (FlatTuple flatTuple = result.getNext()) {
                assertNotNull(flatTuple);

                String str = flatTuple.toString();
                assertTrue(str.equals("Alice|35|1.731000\n"));
            }
        }
    }

}
