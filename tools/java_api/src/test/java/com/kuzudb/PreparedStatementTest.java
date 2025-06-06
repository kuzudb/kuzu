package com.kuzudb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

import java.util.Map;

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
    void PrepStmtSpecialString() {
        String query1 = "MATCH (n:movies) WHERE n.name = 'The 😂😃🧘🏻‍♂️🌍🌦️🍞🚗 movie' RETURN n.name";
        try (PreparedStatement preparedStatement1 = conn.prepare(query1)) {
            QueryResult result = conn.execute(preparedStatement1, Map.of());
            while (result.hasNext()) {
                String got = result.getNext().getValue(0).getValue();
                assertTrue(got.equals("The 😂😃🧘🏻‍♂️🌍🌦️🍞🚗 movie"));
            }
        }
        String query2 = "MATCH (n:movies) WHERE n.name = $1 RETURN n.name";
        Map<String, Value> params = Map.of("1", new Value("The 😂😃🧘🏻‍♂️🌍🌦️🍞🚗 movie"));
        try (PreparedStatement preparedStatement2 = conn.prepare(query2)) {
            QueryResult result = conn.execute(preparedStatement2, params);
            while (result.hasNext()) {
                String got = result.getNext().getValue(0).getValue();
                assertTrue(got.equals("The 😂😃🧘🏻‍♂️🌍🌦️🍞🚗 movie"));
            }
        }
    }

}
