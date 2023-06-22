package tools.java_api.java_test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Files;


import tools.java_api.*;

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
        Path tempDir = Files.createTempDirectory("java_api_test_db");
        tempDir.toFile().deleteOnExit();

        BufferedReader reader;
        db = new KuzuDatabase(dbPath, 0);
        conn = new KuzuConnection(db);
        try {
			reader = new BufferedReader(new FileReader("./../../dataset/tinysnb/schema.cypher"));
			String line = reader.readLine();

			while (line != null) {
				conn.query(line);
				line = reader.readLine();
			}

            reader.close();

            reader = new BufferedReader(new FileReader("./../../dataset/tinysnb/copy.cypher"));
            line = reader.readLine();

            while (line != null) {
				conn.query(line);
				line = reader.readLine();
			}

			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
    }
}
