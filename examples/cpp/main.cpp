#include <iostream>
#include <chrono>

#include "kuzu.hpp"
using namespace kuzu::main;

void socSingleSequentialBatch(std::unique_ptr<Connection>& connection) {
    connection->query("COPY Link FROM '../../../experiments/soc/sequential.csv' (DELIM=',');");
}

void socSingleShuffledBatch(std::unique_ptr<Connection>& connection) {
    connection->query("COPY Link FROM '../../../experiments/soc/shuffled.csv' (DELIM=',');");
}

void singleSequentialBatch(std::unique_ptr<Connection>& connection) {
    connection->query("COPY Link FROM '../../../experiments/sequential.csv' (DELIM=',');");
}

void singleSequentialBatchExceptLast(std::unique_ptr<Connection>& connection) {
    connection->query("COPY Link FROM '../../../experiments/sequential-missing-last-1m.csv' (DELIM=',');");
}

void single1m(std::unique_ptr<Connection>& connection) {
    connection->query("COPY Link FROM '../../../experiments/sequential-last-1m.csv' (DELIM=',');");
}

void singleShuffledBatch(std::unique_ptr<Connection>& connection) {
    connection->query("COPY Link FROM '../../../experiments/shuffled.csv' (DELIM=',');");
}


// soc 1k
void epiniumsSequentialBatch1k(std::unique_ptr<Connection>& connection) {
	int totalTime = 0;
//    connection->query("BEGIN TRANSACTION;");
    for (auto i = 1; i <= 509; i++) {
        std::string filename = "seq-" + std::to_string(i) + ".csv";
        std::string query = "COPY Link FROM 'experiments/soc/1k-sequential/" + filename + "' (DELIM=',');";

        auto start = std::chrono::high_resolution_clock::now();
        connection->query(query);
//        connection->query("CHECKPOINT;");
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
		totalTime += duration.count();
        std::cout << i << "," << totalTime << std::endl;
    }
 //   connection->query("COMMIT;");
}

void epiniumsShuffledBatch1k(std::unique_ptr<Connection>& connection) {
	int totalTime = 0;
    for (auto i = 1; i <= 509; i++) {
        std::string filename = "seq-" + std::to_string(i) + ".csv";
        std::string query = "COPY Link FROM 'experiments/soc/1k-shuffled/" + filename + "' (DELIM=',');";
        auto start = std::chrono::high_resolution_clock::now();

        connection->query(query);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
		totalTime += duration.count();
        std::cout << i << "," << totalTime << std::endl;
    }
}

// 1m 

void insertFile(std::unique_ptr<Connection>& connection, std::string filename) {
    std::string query = "COPY Link FROM '../../../experiments/1m-sequential/" + filename + "' (DELIM=',');";
    connection->query(query);
}

void sequentialBatch1m(std::unique_ptr<Connection>& connection) {
	int totalTime = 0;
    for (auto i = 1; i <= 118; i++) {
        std::string filename = "seq-" + std::to_string(i) + ".csv";
        std::string query = "COPY Link FROM 'experiments/1m-sequential/" + filename + "' (DELIM=',');";

		std::cout << query << std::endl;
        auto start = std::chrono::high_resolution_clock::now();
        connection->query(query);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
		totalTime += duration.count();
        std::cout << i << "," << totalTime << std::endl;
    }
}

void shuffledBatch1m(std::unique_ptr<Connection>& connection) {
	int totalTime = 0;
    for (auto i = 1; i <= 118; i++) {
        std::string filename = "seq-" + std::to_string(i) + ".csv";
        std::string query = "COPY Link FROM 'experiments/1m-shuffled-entire/" + filename + "' (DELIM=',');";
		std::cout << query << std::endl;
        auto start = std::chrono::high_resolution_clock::now();
        connection->query(query);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
		totalTime += duration.count();
        std::cout << i << "," << totalTime << std::endl;
    }
}

// 300k
void sequentialBatch300k(std::unique_ptr<Connection>& connection) {
    for (auto i = 1; i <= 391; i++) {
        std::string filename = "seq-" + std::to_string(i) + ".csv";
        std::string query = "COPY Link FROM '../../../experiments/300k-sequential/" + filename + "' (DELIM=',');";
        connection->query(query);
    }
}

void shuffledBatch300k(std::unique_ptr<Connection>& connection) {
	int totalTime = 0;
    for (auto i = 1; i <= 391; i++) {
        std::string filename = "seq-" + std::to_string(i) + ".csv";
        std::string query = "COPY Link FROM '../../../experiments/300k-shuffled-entire/" + filename + "' (DELIM=',');";
        auto start = std::chrono::high_resolution_clock::now();
        connection->query(query);
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
		totalTime += duration.count();
        std::cout << i << "," << totalTime << std::endl;
    }
}


// 30k 
void sequentialBatch30k(std::unique_ptr<Connection>& connection) {
    for (auto i = 1; i <= 3907; i++) {
        std::string filename = "seq-" + std::to_string(i) + ".csv";
        std::string query = "COPY Link FROM '../../../experiments/30k-sequential/" + filename + "' (DELIM=',');";
        connection->query(query);
    }
}

void shuffledBatch30k(std::unique_ptr<Connection>& connection) {
    for (auto i = 1; i <= 3907; i++) {
        std::string filename = "seq-" + std::to_string(i) + ".csv";
        std::string query = "COPY Link FROM '../../../experiments/30k-shuffled-entire/" + filename + "' (DELIM=',');";
        connection->query(query);
    }
}

void buildOrkut() {
	system("rm -rf /tmp/mydb");
    auto database = std::make_unique<Database>("/tmp/mydb");
    auto connection = std::make_unique<Connection>(database.get());
	std::cout << "build orku" << std::endl;
    connection->query("CREATE NODE TABLE Node(id INT64, PRIMARY KEY (id));");
    connection->query("CREATE REL TABLE Link(FROM Node TO Node);");
    connection->query("COPY Node FROM 'experiments/nodes.csv';");
}

void orkut(std::string filename) {
    auto database = std::make_unique<Database>("/tmp/mydb");
    auto connection = std::make_unique<Connection>(database.get());
	std::string f = "seq-" + filename + ".csv";
	std::string query = "COPY Link FROM 'experiments/1m-shuffled-entire/" + f + "' (DELIM=',');";
	connection->query(query);
}

void orkutWithNewConnections() {
	std::cout << "Orkut insert" << std::endl;
    auto database = std::make_unique<Database>("/tmp/mydb");
    auto connection = std::make_unique<Connection>(database.get());

    connection->query("CREATE NODE TABLE Node(id INT64, PRIMARY KEY (id));");
    connection->query("CREATE REL TABLE Link(FROM Node TO Node);");
    connection->query("COPY Node FROM 'experiments/nodes.csv';");

    auto start = std::chrono::high_resolution_clock::now();

	int totalTime = 0;
    for (auto i = 1; i <= 118; i++) {
		database = std::make_unique<Database>("/tmp/mydb");
		connection = std::make_unique<Connection>(database.get());
        std::string filename = "seq-" + std::to_string(i) + ".csv";
        std::string query = "COPY Link FROM 'experiments/1m-shuffled-entire/" + filename + "' (DELIM=',');";
        // std::string query = "COPY Link FROM 'experiments/1m-sequential/" + filename + "' (DELIM=',');";
		std::cout << query << std::endl;
        auto startLocal = std::chrono::high_resolution_clock::now();
        connection->query(query);
        auto endLocal = std::chrono::high_resolution_clock::now();
        auto durationLocal = std::chrono::duration_cast<std::chrono::milliseconds>(endLocal - startLocal);
		totalTime += durationLocal.count();
        std::cout << i << "," << totalTime << std::endl;
    }

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Time taken by function: "
        << duration.count() << " milliseconds" << std::endl;
}



void orkut() {
	system("rm -rf /tmp/mydb");
	std::cout << "Orkut insert" << std::endl;
    auto database = std::make_unique<Database>("/tmp/mydb");
    auto connection = std::make_unique<Connection>(database.get());

    connection->query("CREATE NODE TABLE Node(id INT64, PRIMARY KEY (id));");
    connection->query("CREATE REL TABLE Link(FROM Node TO Node);");
    connection->query("COPY Node FROM 'experiments/nodes.csv';");

    // connection->query("CALL THREADS=32;");
    // connection->query("CALL THREADS=1;");

    auto start = std::chrono::high_resolution_clock::now();

	sequentialBatch1m(connection);
//	shuffledBatch1m(connection);
//    single1m(connection);
//	shuffledBatch300k(connection);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Time taken by function: "
        << duration.count() << " milliseconds" << std::endl;
}

void soc2() {
	system("rm -rf /tmp/mydb");
    SystemConfig systemConfig;
    systemConfig.enableCompression = false;
	auto database = std::make_unique<Database>("/tmp/mydb", systemConfig);
	auto connection = std::make_unique<Connection>(database.get());

    connection->query("CREATE NODE TABLE Node(id INT64, PRIMARY KEY (id));");
	connection->query("CREATE REL TABLE Link(FROM Node TO Node);");
	connection->query("COPY Node FROM 'experiments/nodes.csv';");
//	connection->query("CALL THREADS=1;");

    auto start = std::chrono::high_resolution_clock::now();
    connection->query("COPY Link FROM 'experiments/tail.csv' (DELIM=',');");
    connection->query("COPY Link FROM 'experiments/head.csv' (DELIM=',');");
    auto end = std::chrono::high_resolution_clock::now();

    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Time taken by function: " << duration.count() << " milliseconds" << std::endl;
}

void dummyQuery() { 
    SystemConfig systemConfig;
    systemConfig.enableCompression = false;
    auto database = std::make_unique<Database>("dummy", systemConfig);
    auto connection = std::make_unique<Connection>(database.get());
    auto result = connection->query("MATCH (n:Person)-[r:Friendship]-(n1:Person) RETURN r;");
    std::cout << result->toString() << std::endl;
}

void dummy() {
   dummyQuery();
    return;
	system("rm -rf dummy");
    SystemConfig systemConfig;
    systemConfig.enableCompression = false;
    auto database = std::make_unique<Database>("dummy", systemConfig);
    auto connection = std::make_unique<Connection>(database.get());
    connection->query("CREATE NODE TABLE Person(id INT64, name STRING, PRIMARY KEY (id));");
	connection->query("CREATE REL TABLE Friendship(FROM Person TO Person, christmas_toy STRING);");
    connection->query("CHECKPOINT;");
    connection->query("CREATE (u:Person {id: 0, name: 'Jill'});");
    connection->query("CREATE (u:Person {id: 1, name: 'Alice'});");
    connection->query("CREATE (u:Person {id: 2, name: 'Bob'});");
//    connection->query("MATCH (u1:Person), (u2:Person) WHERE u1.name = 'Alice' AND u2.name = 'Bob' CREATE (u1)-[:Friendship {christmas_toy: 'Gingerbread house'}]->(u2);");
//    connection->query("MATCH (n:Person)-[r:Friendship]->(n1:Person) RETURN r.christmas_toy;");
    connection->query("COPY Friendship FROM '/Users/rfdavid/Devel/waterloo/tmp/rel.csv'");
    auto result = connection->query("MATCH (n:Person)-[r:Friendship]-(n1:Person) RETURN r;");
//    connection->query("CREATE (u:Person {id: 2, name: 'Jill'});");
    std::cout << result->toString() << std::endl;
}

void soc() {
	std::cout << "SOC-Epiniums insert" << std::endl;
	system("rm -rf /tmp/mydb");
    SystemConfig systemConfig;
    systemConfig.enableCompression = false;
    // systemConfig.checkpointThreshold = 999999999999999;
	auto database = std::make_unique<Database>("/tmp/mydb", systemConfig);
	auto connection = std::make_unique<Connection>(database.get());

    connection->query("CREATE NODE TABLE Node(id INT64, PRIMARY KEY (id));");
	connection->query("CREATE REL TABLE Link(FROM Node TO Node);");
	connection->query("COPY Node FROM 'experiments/soc/nodes.csv';");

//	connection->query("CALL THREADS=12;");
	connection->query("CALL THREADS=1;");

    auto start = std::chrono::high_resolution_clock::now();

//    epiniumsSequentialBatch1k(connection);
    epiniumsShuffledBatch1k(connection);
//    socSingleSequentialBatch(connection);

    auto end = std::chrono::high_resolution_clock::now();
    auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start);
    std::cout << "Time taken by function: "
        << duration.count() << " milliseconds" << std::endl;
}

int main(int argc, char *argv[]) {
    if (argc > 1) {
        std::string arg = argv[1];
		if (arg == "build") {
			buildOrkut();
		} else {
			orkut(argv[1]);
		}
    } else {
        dummy();
        // soc();
        // orkut();
        // orkutWithNewConnections();
    }
}
