package tools.java_api.java_test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;


import tools.java_api.*;

public class TestHelper {
    private static KuzuDatabase db;
    private static KuzuConnection conn;

    private static void deleteFolder(File folder) {
        if (folder.exists()) {
            File[] files = folder.listFiles();
            if (files != null) {
                for (File file : files) {
                    if (file.isDirectory()) {
                        deleteFolder(file);
                    } else {
                        file.delete();
                    }
                }
            }
            folder.delete();
        }
    }

    public static void cleanup() {
        String folderPath = "java_api_test_db";
        deleteFolder(new File(folderPath));
    }

    public static KuzuDatabase getDatabase() {
        return db;
    }

    public static KuzuConnection getConnection() {
        return conn;
    }

    public static void loadData() {
        BufferedReader reader;
        db = new KuzuDatabase("java_api_test_db", 0);
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
