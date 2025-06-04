package com.kuzudb;

import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

public class TestTest extends TestBase {
    @Test
    void TestTestTest() throws Exception {
        conn.query("create node table person (int64 id primary key)");
        conn.query("create rel table knows (from person to person, DATE date, TIMESTAMP meetTime)");
        
        try {
            this.runQuery(conn,
                "match ()-[r:knows]->() where id(r)=INTERNAL_ID(3,3) set r.date=$p",
                // Instant.now() // error
                LocalDate.now() // works nicely
            );

            this.runQuery(conn,
                "match ()-[r:knows]->() where id(r)=INTERNAL_ID(3,3) set r.meetTime=$p",
                // Instant.now() - sets invalid date in the year 1970
                LocalDateTime.now() // error
                // LocalDate.now() // sets today but with 00:00 time
            );
        } catch (Exception e) {
            e.printStackTrace(System.out);
        }
    }

    public void runQuery(Connection connection, String query, Object parameterValue) throws Exception {
        Map<String, Value> parameters = new HashMap<>();
        parameters.put("p", new Value(parameterValue));
        PreparedStatement preparedStatement = connection.prepare(query);
        QueryResult result = connection.execute(preparedStatement, parameters);
        int count = 0;
        while (result.hasNext()) {
            result.getNext();
            count++;
        }
        System.out.println("Number of results: " + count);
    }
}
