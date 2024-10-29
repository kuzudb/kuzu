package com.kuzudb;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class PreparedStatementTest extends TestBase {

    @Test
    void PrepStmtIsSuccess() throws ObjectRefDestroyedException {
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
    void PrepStmtGetErrorMessage() throws ObjectRefDestroyedException {
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

}
