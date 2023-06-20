package tools.java_api.java_test;

import tools.java_api.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.Scanner;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;

public class QueryResultTest extends TestBase {

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
        assertTrue(errorMessage.equals("Binder exception: Node table personnnn does not exist."));
        result.destroy();

        System.out.println("QueryResultGetErrorMessage passed");
    }

    @Test
    void QueryResultGetNumColumns() throws KuzuObjectRefDestroyedException {
        KuzuQueryResult result = conn.query("MATCH (a:person) RETURN a.fName, a.age, a.height");
        assertTrue(result.isSuccess());
        assertEquals(result.getNumColumns(), 3);
        result.destroy();

        System.out.println("QueryResultGetNumColumns passed");
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

        System.out.println("QueryResultGetColumnName passed");
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

        System.out.println("QueryResultGetColumnDataType passed");
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

        System.out.println("QueryResultGetQuerySummary passed");
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

        System.out.println("QueryResultGetNext passed");
    }

    @Test
    void QueryResultWriteToCSV() throws IOException, KuzuObjectRefDestroyedException{
        String newline = "\n";
        String basicOutput =
            "Carol,1,5.000000,1940-06-22,1911-08-20 02:32:21,CsWork" + newline +
            "Dan,2,4.800000,1950-07-23,2031-11-30 12:25:30,DEsWork" + newline +
            "Elizabeth,1,4.700000,1980-10-26,1976-12-23 11:21:42,DEsWork" + newline;
        String query = "MATCH (a:person)-[:workAt]->(o:organisation) RETURN a.fName, a.gender," +
                       "a.eyeSight, a.birthdate, a.registerTime, o.name";
        KuzuQueryResult result = conn.query(query);
        assertTrue(result.isSuccess());

        Path tempDir = Files.createTempFile("output_CSV_CAPI", "csv");
        tempDir.toFile().deleteOnExit();

        String outputPath = "output_CSV_CAPI.csv";
        result.writeToCsv(outputPath, ',', '"', '\n');
        
        try {
            File csv = new File(outputPath);
            Scanner s = new Scanner(csv);
            String content = s.useDelimiter("\\z").next();
            assertTrue(basicOutput.equals(content));
            s.close();
            System.out.println("QueryResultWriteToCSV passed");
        } catch(FileNotFoundException e) {
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

        System.out.println("QueryResultResetIterator passed");
    }
}
