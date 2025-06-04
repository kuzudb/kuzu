package com.kuzudb;

import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class PreparedStatementTest extends TestBase {

    @Test
    void PrepStmtIsSuccess() {
        String query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
        try (PreparedStatement preparedStatement1 = conn.prepare(query)) {
            assertNotNull(preparedStatement1);
            assertTrue(preparedStatement1.isSuccess());
        }

        query = "MATCH (a:personnnn) WHERE a.isStudent = $1 RETURN COUNT(*)";
        try (PreparedStatement preparedStatement2 = conn.prepare(query)) {
            assertNotNull(preparedStatement2);
            assertFalse(preparedStatement2.isSuccess());
        }
    }

    @Test
    void PrepStmtGetErrorMessage() {
        String query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
        try (PreparedStatement preparedStatement1 = conn.prepare(query)) {
            assertNotNull(preparedStatement1);
            String message = preparedStatement1.getErrorMessage();
            assertTrue(message.equals(""));
        }

        query = "MATCH (a:personnnn) WHERE a.isStudent = $1 RETURN COUNT(*)";
        try (PreparedStatement preparedStatement2 = conn.prepare(query)) {
            assertNotNull(preparedStatement2);
            String message = preparedStatement2.getErrorMessage();
            assertTrue(message.equals("Binder exception: Table personnnn does not exist."));
        }
    }

    @Test
    void PrepStmtTimestamp() {
        Instant time = Instant.now();
        Map<String, Value> parameters = Map.of("p", new Value(time));
        String query = "MATCH ()-[r:knows]->() SET r.meetTime=$p RETURN r.meetTime";
        try (PreparedStatement preparedStatement = conn.prepare(query)) {
            String message = preparedStatement.getErrorMessage();
            assertTrue(message.equals(""));
            QueryResult result = conn.execute(preparedStatement, parameters);
            while (result.hasNext()) {
                String got = result.getNext().getValue(0).getValue().toString();
                assertTrue(got.equals(time.toString()));
            }
        }
    }

}
