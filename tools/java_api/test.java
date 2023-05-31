package tools.java_api;

public class test {
    public static void main(String[] args) {
        KuzuDatabase db = new KuzuDatabase("./testdb/testdb.db", 20);
        KuzuConnection connection = new KuzuConnection(db);
        
        connection.beginWriteTransaction();
        connection.query("CREATE NODE TABLE Person3(name STRING, age INT64, PRIMARY KEY(name));");
        // Create nodes.
        connection.query("CREATE (:Person {name: 'Alice', age: 25});");
        connection.query("CREATE (:Person {name: 'Bob', age: 30});");
        System.out.println("connection end");
        KuzuQueryResult result = connection.query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;");
        // Print query result.
        System.out.println("result end");
        System.out.println(result.getColumnName(0));
        System.out.println(result.toString());
        

        KuzuDataTypeID id =  KuzuDataTypeID.DOUBLE;
        KuzuDataType dt = new KuzuDataType(id);

        System.out.println(dt.getID());
    }
}
