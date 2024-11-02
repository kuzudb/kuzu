package com.kuzudb;

import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;

public class QueryResultTest extends TestBase {
    @TempDir
    static Path tempDir;

    @Test
    void QueryResultGetErrorMessage() throws ObjectRefDestroyedException {
        try (QueryResult result = conn.query("MATCH (a:person) RETURN COUNT(*)")) {
            assertTrue(result.isSuccess());
            String errorMessage = result.getErrorMessage();
            assertTrue(errorMessage.equals(""));
        }

        try (QueryResult result = conn.query("MATCH (a:personnnn) RETURN COUNT(*)")) {
            assertFalse(result.isSuccess());
            String errorMessage = result.getErrorMessage();
            assertTrue(errorMessage.equals("Binder exception: Table personnnn does not exist."));
        }
    }

    @Test
    void QueryResultGetNumColumns() throws ObjectRefDestroyedException {
        try (QueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age, a.height")) {
            assertTrue(result.isSuccess());
            assertEquals(result.getNumColumns(), 3);
        }
    }

    @Test
    void QueryResultGetColumnName() throws ObjectRefDestroyedException {
        try (QueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age, a.height")) {
            assertTrue(result.isSuccess());

            String columnName = result.getColumnName(0);
            assertTrue(columnName.equals("a.fName"));

            columnName = result.getColumnName(1);
            assertTrue(columnName.equals("a.age"));

            columnName = result.getColumnName(2);
            assertTrue(columnName.equals("a.height"));

            columnName = result.getColumnName(222);
            assertNull(columnName);
        }
    }

    @Test
    void QueryResultGetColumnDataType() throws ObjectRefDestroyedException {
        try (QueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age, a.height, cast(1 as decimal)")) {
            assertTrue(result.isSuccess());

            try (DataType type = result.getColumnDataType(0)) {
                assertEquals(type.getID(), DataTypeID.STRING);
            }

            try (DataType type = result.getColumnDataType(1)) {
                assertEquals(type.getID(), DataTypeID.INT64);
            }

            try (DataType type = result.getColumnDataType(2)) {
                assertEquals(type.getID(), DataTypeID.FLOAT);
            }

            try (DataType type = result.getColumnDataType(3)) {
                assertEquals(type.getID(), DataTypeID.DECIMAL);
            }

            DataType type = result.getColumnDataType(222);
            assertNull(type);
        }
    }

    @Test
    void QueryResultGetQuerySummary() throws ObjectRefDestroyedException {
        try (QueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age, a.height")) {
            assertTrue(result.isSuccess());

            QuerySummary summary = result.getQuerySummary();
            assertNotNull(summary);

            double compilingTime = summary.getCompilingTime();
            assertTrue(compilingTime > 0);

            double executionTime = summary.getExecutionTime();
            assertTrue(executionTime > 0);
        }
    }

    @Test
    void QueryResultGetNext() throws ObjectRefDestroyedException {
        try (QueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age ORDER BY a.fName")) {
            assertTrue(result.isSuccess());

            assertTrue(result.hasNext());
            try (FlatTuple row = result.getNext()) {
                assertNotNull(row);
                assertTrue(row.getValue(0).getValue().equals("Alice"));
                assertTrue(row.getValue(1).getValue().equals(35L));
            }

            assertTrue(result.hasNext());
            try (FlatTuple row = result.getNext()) {
                assertNotNull(row);
                assertTrue(row.getValue(0).getValue().equals("Bob"));
                assertTrue(row.getValue(1).getValue().equals(30L));
            }
        }
    }

    @Test
    void QueryResultResetIterator() throws ObjectRefDestroyedException {
        try (QueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age ORDER BY a.fName")) {
            assertTrue(result.isSuccess());
            assertTrue(result.hasNext());

            try (FlatTuple row = result.getNext()) {
                assertTrue(row.getValue(0).getValue().equals("Alice"));
                assertTrue(row.getValue(1).getValue().equals(35L));
            }

            result.resetIterator();

            assertTrue(result.hasNext());
            try (FlatTuple row = result.getNext()) {
                assertNotNull(row);
                assertTrue(row.getValue(0).getValue().equals("Alice"));
                assertTrue(row.getValue(1).getValue().equals(35L));
            }
        }
    }

    @Test
    void getMultipleQueryResult() throws ObjectRefDestroyedException {
        try (QueryResult result = conn.query("return 1; return 2; return 3;")) {
            assertTrue(result.isSuccess());
            String str = result.toString();
            assertEquals(str, "1\n1\n");

            assertTrue(result.hasNextQueryResult());
            try (QueryResult nextResult = result.getNextQueryResult()) {
                str = nextResult.toString();
                assertEquals(str, "2\n2\n");
            }

            assertTrue(result.hasNextQueryResult());
            try (QueryResult nextResult = result.getNextQueryResult()) {
                str = nextResult.toString();
                assertEquals(str, "3\n3\n");
            }

            assertFalse(result.hasNextQueryResult());
        }
    }
}
