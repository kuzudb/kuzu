package com.kuzudb.java_test;

import com.kuzudb.*;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

public class PreparedStatementTest extends TestBase {

    @Test
    void PrepStmtIsSuccess() throws KuzuObjectRefDestroyedException {
        String query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
        KuzuPreparedStatement preparedStatement1 = conn.prepare(query);
        assertNotNull(preparedStatement1);
        assertTrue(preparedStatement1.isSuccess());
        preparedStatement1.destroy();

        query = "MATCH (a:personnnn) WHERE a.isStudent = $1 RETURN COUNT(*)";
        KuzuPreparedStatement preparedStatement2 = conn.prepare(query);
        assertNotNull(preparedStatement2);
        assertFalse(preparedStatement2.isSuccess());
        preparedStatement2.destroy();
    }

    @Test
    void PrepStmtGetErrorMessage() throws KuzuObjectRefDestroyedException {
        String query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
        KuzuPreparedStatement preparedStatement1 = conn.prepare(query);
        assertNotNull(preparedStatement1);
        String message = preparedStatement1.getErrorMessage();
        assertTrue(message.equals(""));
        preparedStatement1.destroy();

        query = "MATCH (a:personnnn) WHERE a.isStudent = $1 RETURN COUNT(*)";
        KuzuPreparedStatement preparedStatement2 = conn.prepare(query);
        assertNotNull(preparedStatement2);
        message = preparedStatement2.getErrorMessage();
        assertTrue(message.equals("Binder exception: Table personnnn does not exist."));
        preparedStatement2.destroy();
    }

}
