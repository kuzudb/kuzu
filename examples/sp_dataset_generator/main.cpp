#include <fstream>
#include <iostream>
#include <string>
//#include "kuzu.hpp"
// using namespace kuzu::main;
using namespace std;

int main() {
    std::cout << "Hello World!" << std::endl;
    ofstream outFile("/Users/ssalihog/data/test_shortest_paths/4-hop-k-10.csv");
    string line;
    for (int i = 10; i <= 19; ++i) {
        outFile << 0 << " " << i << endl;
    }
    for (int i = 10; i <= 19; ++i) {
        for (int j = 0; j < 10; ++j) {
            outFile << i << " " << i*10+j << endl;
        }
    }
    for (int i = 100; i <= 199; ++i) {
        for (int j = 0; j < 10; ++j) {
            outFile << i << " " << i*10+j << endl;
        }
    }
    outFile << line << endl;
    outFile.close();

//    if (myfile.is_open()) {
//        string str;
//        do {
//            getline(cin, str);
//            myfile << str << endl;
//        } while (str != "");
//        myfile.close();
//    } else
//        cerr << "Unable to open file";

    return 0;

    //    auto database = std::make_unique<Database>("" /* fill db path */);
    //    auto connection = std::make_unique<Connection>(database.get());
    //
    //    // Create schema.
    //    connection->query("CREATE NODE TABLE Person(name STRING, age INT64, PRIMARY KEY(name));");
    //    // Create nodes.
    //    connection->query("CREATE (:Person {name: 'Alice', age: 25});");
    //    connection->query("CREATE (:Person {name: 'Bob', age: 30});");
    //
    //    // Execute a simple query.
    //    auto result = connection->query("MATCH (a:Person) RETURN a.name AS NAME, a.age AS AGE;");
    // Print query result.
    //    std::cout << result->toString();
}
