import com.kuzudb.*;

public class Main {
    public static void main(String[] args) {
        // Create an in-memory database and connect to it
        Database db = new Database(":memory:");
        Connection conn = new Connection(db);
        // Create tables.
        conn.query("CREATE NODE TABLE User(name STRING, age INT64, PRIMARY KEY (name))");
        conn.query("CREATE NODE TABLE City(name STRING, population INT64, PRIMARY KEY (name))");
        conn.query("CREATE REL TABLE Follows(FROM User TO User, since INT64)");
        conn.query("CREATE REL TABLE LivesIn(FROM User TO City)");
        // Load data.
        conn.query("COPY User FROM '../../../dataset/demo-db/csv/user.csv'");
        conn.query("COPY City FROM '../../../dataset/demo-db/csv/city.csv'");
        conn.query("COPY Follows FROM '../../../dataset/demo-db/csv/follows.csv'");
        conn.query("COPY LivesIn FROM '../../../dataset/demo-db/csv/lives-in.csv'");

        // Execute a simple query.
        QueryResult result =
                conn.query("MATCH (a:User)-[f:Follows]->(b:User) RETURN a.name, f.since, b.name;");
        while (result.hasNext()) {
            FlatTuple row = result.getNext();
            System.out.print(row);
        }
    }
}
