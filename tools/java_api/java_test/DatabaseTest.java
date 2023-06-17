package tools.java_api.java_test;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import tools.java_api.*;

public class DatabaseTest {
    @Test
    void DBCreationAndDestroy() {
        String databasePath = System.getProperty("user.dir") + "dbtest";
        try{
            KuzuDatabase database = new KuzuDatabase(databasePath, 0);
            database.destory();
        }catch(Exception e) {
            fail("DBCreationAndDestroy failed: ");
            System.out.println(e.toString());
        }

        System.out.println("DBCreationAndDestroy passed");
    }

    @Test 
    void DBInvalidPath() {
        try{
            KuzuDatabase database = new KuzuDatabase("", 0);
            database.destory();
            fail("DBInvalidPath did not throw exception as expected.");
        }catch(Exception e) {
            System.out.println("DBInvalidPath passed");
        }
    }

    @Test
    void DBSetLoggingLevel() {
        try{
            KuzuDatabase.setLoggingLevel("debug");
            KuzuDatabase.setLoggingLevel("info");
            KuzuDatabase.setLoggingLevel("err");
        }catch(Exception e){
            fail("DBSetLoggingLevel failed: ");
            System.out.println(e.toString());
        }
        
        try{
            KuzuDatabase.setLoggingLevel("unsupported");
            fail("DBSetLoggingLevel did not throw exception as expected.");
        }catch(Exception e){
        }

        System.out.println("DBSetLoggingLevel passed");
    }
}
