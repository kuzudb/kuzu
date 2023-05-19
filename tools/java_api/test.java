package tools.java_api;

public class test {
    public static void main(String[] args) {
        KuzuDatabase db = new KuzuDatabase("./testdb/testdb.db", 20);
        String lvl = "test";
        db.destoryDatabase();
        db.setLoggingLevel(lvl, db);
    }
}
