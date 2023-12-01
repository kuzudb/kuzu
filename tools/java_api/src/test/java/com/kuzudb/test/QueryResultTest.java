package com.kuzudb.java_test;

import com.kuzudb.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;

public class QueryResultTest extends TestBase {
    @TempDir
    static Path tempDir;

    @Test
    void QueryResultGetErrorMessage() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN COUNT(*)");
        assertTrue(result.isSuccess());
        String errorMessage = result.getErrorMessage();
        assertTrue(errorMessage.equals(""));
        result.destroy();

        result = conn.query("MATCH (a:personnnn) RETURN COUNT(*)");
        assertFalse(result.isSuccess());
        errorMessage = result.getErrorMessage();
        assertTrue(errorMessage.equals("Binder exception: Table personnnn does not exist."));
        result.destroy();
    }

    @Test
    void QueryResultGetNumColumns() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age, a.height");
        assertTrue(result.isSuccess());
        assertEquals(result.getNumColumns(), 3);
        result.destroy();
    }

    @Test
    void QueryResultGetColumnName() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age, a.height");
        assertTrue(result.isSuccess());
        String columnName = result.getColumnName(0);
        assertTrue(columnName.equals("a.fName"));

        columnName = result.getColumnName(1);
        assertTrue(columnName.equals("a.age"));

        columnName = result.getColumnName(2);
        assertTrue(columnName.equals("a.height"));

        columnName = result.getColumnName(222);
        assertNull(columnName);

        result.destroy();
    }

    @Test
    void QueryResultGetColumnDataType() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age, a.height");
        assertTrue(result.isSuccess());
        KuzuDataType type = result.getColumnDataType(0);
        assertEquals(type.getID(), KuzuDataTypeID.STRING);
        type.destroy();

        type = result.getColumnDataType(1);
        assertEquals(type.getID(), KuzuDataTypeID.INT64);
        type.destroy();
        type = result.getColumnDataType(2);
        assertEquals(type.getID(), KuzuDataTypeID.FLOAT);
        type.destroy();

        type = result.getColumnDataType(222);
        assertNull(type);
    }

    @Test
    void QueryResultGetQuerySummary() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age, a.height");
        assertTrue(result.isSuccess());
        KuzuQuerySummary summary = result.getQuerySummary();
        assertNotNull(summary);
        double compilingTime = summary.getCompilingTime();
        assertTrue(compilingTime > 0);
        double executionTime = summary.getExecutionTime();
        assertTrue(executionTime > 0);
        result.destroy();
    }

    @Test
    void QueryResultGetNext() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age ORDER BY a.fName");
        assertTrue(result.isSuccess());

        assertTrue(result.hasNext());
        KuzuFlatTuple row = result.getNext();
        assertNotNull(row);
        assertTrue(row.getValue(0).getValue().equals("Alice"));
        assertTrue(row.getValue(1).getValue().equals(35L));
        row.destroy();

        assertTrue(result.hasNext());
        row = result.getNext();
        assertNotNull(row);
        assertTrue(row.getValue(0).getValue().equals("Bob"));
        assertTrue(row.getValue(1).getValue().equals(30L));
        row.destroy();

        result.destroy();
    }

    @Test
    void QueryResultWriteToCSV() throws IOException, KuzuObjectRefDestroyedException {
        String query = "MATCH (a:person)-[:workAt]->(o:organisation) RETURN a.fName, a.gender," +
                "a.eyeSight, a.birthdate, a.registerTime, o.name";
        KuzuQueryResult result = conn.query(query);
        assertTrue(result.isSuccess());

        final Path tempFile = Files.createFile(tempDir.resolve("test.csv"));
        String outputPath = tempFile.toFile().getAbsolutePath();
        result.writeToCsv(outputPath, ',', '"', '\n');

        try {
            File csv = new File(outputPath);
            Scanner scanner = new Scanner(csv);
            String line = scanner.nextLine();
            assertEquals(line, "Carol,1,5.000000,1940-06-22,1911-08-20 02:32:21,CsWork");
            line = scanner.nextLine();
            assertEquals(line, "Dan,2,4.800000,1950-07-23,2031-11-30 12:25:30,DEsWork");
            line = scanner.nextLine();
            assertEquals(line, "Elizabeth,1,4.700000,1980-10-26,1976-12-23 11:21:42,DEsWork");
            scanner.close();
        } catch (FileNotFoundException e) {
            fail("QueryResultWriteToCSV failed, csv file not found");
        }
    }

    @Test
    void QueryResultResetIterator() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age ORDER BY a.fName");
        assertTrue(result.isSuccess());
        assertTrue(result.hasNext());

        KuzuFlatTuple row = result.getNext();
        assertTrue(row.getValue(0).getValue().equals("Alice"));
        assertTrue(row.getValue(1).getValue().equals(35L));
        row.destroy();

        result.resetIterator();

        assertTrue(result.hasNext());
        row = result.getNext();
        assertNotNull(row);
        assertTrue(row.getValue(0).getValue().equals("Alice"));
        assertTrue(row.getValue(1).getValue().equals(35L));
        row.destroy();

        result.destroy();
    }
}
