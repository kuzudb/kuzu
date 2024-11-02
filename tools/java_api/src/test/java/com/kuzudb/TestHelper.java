package com.kuzudb;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;


import com.kuzudb.*;

public class TestHelper {
    private static Database db;
    private static Connection conn;

    public static Database getDatabase() {
        return db;
    }

    public static Connection getConnection() {
        return conn;
    }

    public static void loadData(String dbPath) throws IOException, ObjectRefDestroyedException {
        BufferedReader reader;
        db = new Database(dbPath);
        conn = new Connection(db);

        reader = new BufferedReader(new FileReader("../../dataset/tinysnb/schema.cypher"));
        String line;
        do {
            line = reader.readLine();
            if (line == null) {
                break;
            }
            line = line.replace("dataset/tinysnb", "../../dataset/tinysnb");
            try (QueryResult result = conn.query(line)) {
            }
        } while (line != null);
        reader.close();


        reader = new BufferedReader(new FileReader("../../dataset/tinysnb/copy.cypher"));
        do {
            line = reader.readLine();
            if (line == null) {
                break;
            }
            line = line.replace("dataset/tinysnb", "../../dataset/tinysnb");
            try (QueryResult result = conn.query(line)) {
            }
        } while (line != null);
        reader.close();


        try (QueryResult result = conn.query("create node table moviesSerial (ID SERIAL, name STRING, length INT32, note STRING, PRIMARY KEY (ID));")) {
        }

        try (QueryResult result = conn.query("copy moviesSerial from \"../../dataset/tinysnb-serial/vMovies.csv\"")) {
        }
    }
}
