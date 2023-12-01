package com.kuzudb.java_test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;


import com.kuzudb.*;

public class TestHelper {
    private static KuzuDatabase db;
    private static KuzuConnection conn;

    public static KuzuDatabase getDatabase() {
        return db;
    }

    public static KuzuConnection getConnection() {
        return conn;
    }

    public static void loadData(String dbPath) throws IOException, KuzuObjectRefDestroyedException {
        BufferedReader reader;
        db = new KuzuDatabase(dbPath);
        conn = new KuzuConnection(db);
        KuzuQueryResult result;

        reader = new BufferedReader(new FileReader("../../dataset/tinysnb/schema.cypher"));
        String line;
        do {
            line = reader.readLine();
            if (line == null) {
                break;
            }
            line = line.replace("dataset/tinysnb", "../../dataset/tinysnb");
            result = conn.query(line);
            result.destroy();
        } while (line != null);
        reader.close();


        reader = new BufferedReader(new FileReader("../../dataset/tinysnb/copy.cypher"));
        do {
            line = reader.readLine();
            if (line == null) {
                break;
            }
            line = line.replace("dataset/tinysnb", "../../dataset/tinysnb");
            result = conn.query(line);
            result.destroy();
        } while (line != null);
        reader.close();

        result = conn.query("create node table moviesSerial (ID SERIAL, name STRING, length INT32, note STRING, PRIMARY KEY (ID));");
        result.destroy();
        result = conn.query("copy moviesSerial from \"../../dataset/tinysnb-serial/vMovies.csv\"");
        result.destroy();
    }
}
