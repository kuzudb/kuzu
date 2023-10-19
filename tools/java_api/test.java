package tools.java_api;
import com.kuzudb.*;
import java.time.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.File;

public class test {

    public static void deleteFolder(File folder) {
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
            System.out.println("Folder deleted: " + folder.getAbsolutePath());
        }
    }

    public static void main(String[] args) throws KuzuObjectRefDestroyedException {
        String db_path = "./test_db";
        deleteFolder(new File(db_path));
        BufferedReader reader;
        KuzuDatabase db = new KuzuDatabase(db_path);
        KuzuConnection conn = new KuzuConnection(db);
        try {
			reader = new BufferedReader(new FileReader("../../dataset/tinysnb/schema.cypher"));
			String line = reader.readLine();
			while (line != null) {
                System.out.println(line);
				conn.query(line);
				line = reader.readLine();
			}

            reader.close();

            reader = new BufferedReader(new FileReader("../../dataset/tinysnb/copy.cypher"));
            line = reader.readLine();
            while (line != null) {
                line = line.replace("dataset/tinysnb", "../../dataset/tinysnb");
                System.out.println(line);
				conn.query(line);
				line = reader.readLine();
			}
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

        String query = "MATCH (a:person) RETURN a.fName, a.age ORDER BY a.fName";
        System.out.println(query);

        KuzuQueryResult result = conn.query(query);
        System.out.println("error: " + result.getErrorMessage());
        System.out.println("tuples: " + result.getNumTuples());

        while (result.hasNext()) {
            KuzuFlatTuple row = result.getNext();
            System.out.println("row: " + row);
            row.destroy();
        }
        

        result.destroy();
    
    }
}
