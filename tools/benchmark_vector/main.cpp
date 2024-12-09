#include <fstream>
#include <iostream>
#include <string>
#include <vector>
#include <random>
#include "assert.h"
#include <sys/stat.h>
#include <chrono>

#include "main/kuzu.h"

using namespace kuzu::common;
using namespace kuzu::main;

class InputParser {
public:
    InputParser(int &argc, char **argv) {
        for (int i = 1; i < argc; ++i) {
            this->tokens.emplace_back(argv[i]);
        }
    }

    const std::string &getCmdOption(const std::string &option) const {
        std::vector<std::string>::const_iterator itr;
        itr = std::find(this->tokens.begin(), this->tokens.end(), option);
        if (itr != this->tokens.end() && ++itr != this->tokens.end()) {
            return *itr;
        }
        static const std::string emptyString;
        return emptyString;
    }

private:
    std::vector<std::string> tokens;
};

std::vector<std::string> readQueriesFromFile(const std::string &filePath) {
    std::ifstream inputFile(filePath);
    std::vector<std::string> queries;

    if (!inputFile.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        return queries; // Return empty vector on error
    }

    std::string line;
    while (std::getline(inputFile, line)) {
        queries.push_back(line); // Push each line as a separate query
    }

    inputFile.close();
    return queries;
}

void loadFromFile(const std::string &path, uint8_t *data, size_t size) {
    std::ifstream inputFile(path, std::ios::binary);
    inputFile.read(reinterpret_cast<char *>(data), size);
    inputFile.close();
}

int main(int argc, char **argv) {
    InputParser input(argc, argv);
    std::string databasePath = input.getCmdOption("-databasePath");
    std::string queryPath = input.getCmdOption("-queriesPath");
    std::string gtPath = input.getCmdOption("-gtPath");
    int warmupTimes = stoi(input.getCmdOption("-warmup"));
    int actualTimes = stoi(input.getCmdOption("-actual"));
    int k = stoi(input.getCmdOption("-k"));
//    int runSelectivityQueries = stoi(input.getCmdOption("-runSelectivityQueries"));

//    std::string initQueriesPath = basePath + "/kuzu_init.sql";
//    std::string dataPath = basePath + "/kuzu_data.csv";
//    std::string testQueriesPath = basePath + "/kuzu_queries.sql";
//    std::string databasePath = basePath + "/kuzu_data";
//    std::string gtPath = basePath + "/kuzu_gt.bin";
    std::vector<std::string> testQueries;
    testQueries.push_back(queryPath);

//    std::string baseBenchPath = basePath + "/" + benchSuffix;

//    if (!runSelectivityQueries) {
//        testQueries = {testQueriesPath};
//    } else {
//        // selectivity queries file paths     selectivity = [0.1, 0.25, 0.5, 0.75, 0.9, 0.95, 0.98, 0.99, 0.995]
//        testQueries = {
//                baseBenchPath + "/queries_90.00.sql",
//                baseBenchPath + "/queries_75.00.sql",
//                baseBenchPath + "/queries_50.00.sql",
//                baseBenchPath + "/queries_25.00.sql",
//                baseBenchPath + "/queries_10.00.sql",
//                baseBenchPath + "/queries_5.00.sql",
//                baseBenchPath + "/queries_2.00.sql",
//                baseBenchPath + "/queries_1.00.sql"
////                baseBenchPath + "/queries_0.50.sql"
//        };
//    }

//    if (isCreateSchema) {
//        std::string command = "rm -rf " + databasePath;
//        system(command.c_str());
//    }

    auto db = Database(databasePath);
    auto conn = Connection(&db);
//    if (isCreateSchema) {
//        // read all the schema queries
//        auto initQueries = readQueriesFromFile(initQueriesPath);
//        for (int i = 0; i < initQueries.size(); i++) {
//            printf("Running schema query: %s\n", initQueries[i].c_str());
//            auto start = std::chrono::high_resolution_clock::now();
//            auto res = conn.query(initQueries[i]);
//            // print res
//            while (res->hasNext()) {
//                auto row = res->getNext();
//                printf("%s\n", row->getValue(0)->getValue<std::string>().c_str());
//            }
//            auto end = std::chrono::high_resolution_clock::now();
//            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
//            printf("Schema query time: %lld ms\n", duration);
//        }
//    }

    for (auto &queriesPath: testQueries) {
        printf("Running queries from: %s\n", queriesPath.c_str());
        auto queries = readQueriesFromFile(queriesPath);
        auto queryNumVectors = queries.size();
        vector_id_t *gtVecs = new vector_id_t[queryNumVectors * k];
        loadFromFile(gtPath, reinterpret_cast<uint8_t *>(gtVecs), queryNumVectors * k * sizeof(vector_id_t));

        // Randomly select warmupTimes queries from the queries
//        std::random_device rd;
//        std::mt19937 gen(rd());
//        std::uniform_int_distribution<> dis(0, queries.size() - 1);
        for (int i = 0; i < warmupTimes; i++) {
            printf("Warmup started %d\n", i);
            auto start = std::chrono::high_resolution_clock::now();
            for (int j = 0; j < queries.size(); i++) {
                conn.query(queries[j]);
            }
            // Get the current time after the function call
            auto end = std::chrono::high_resolution_clock::now();
            // Calculate the duration of the function execution
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            printf("Warmup finished\n");
            printf("Warmup time %d: %lld ms\n", i, duration);
        }

        // initialize the timer
        auto start = std::chrono::high_resolution_clock::now();
        auto end = std::chrono::high_resolution_clock::now();
        auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();

        // Run all queries and print avg latency
        printf("Benchmark started\n");
        auto recall = 0;
        auto totalQueries = 0;
        auto totalQueriesSkipped = 0;
        long totalDuration = 0;
        for (int i = 0; i < queries.size(); i++) {
            auto localRecall = 0;
            start = std::chrono::high_resolution_clock::now();
            auto res = conn.query(queries[i]);
            end = std::chrono::high_resolution_clock::now();
            duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            totalDuration += duration;
//            if (res->getNumTuples() < k) {
//                totalQueriesSkipped += 1;
//                continue;
//            }
            assert(res->getNumTuples() >= k);
            auto gt = gtVecs + i * k;
            while (res->hasNext()) {
                auto row = res->getNext();
                auto val = row->getValue(0)->getValue<int64_t>();
                if (std::find(gt, gt + k, val) != (gt + k)) {
                    recall++;
                    localRecall++;
                }
            }
            printf("Recall for query %d: %d\n", i, localRecall);
            printf("duration: %ld ms\n", duration);
            printf("====================================\n");
            totalQueries++;
        }
        printf("Benchmark finished\n");
        printf("Total queries: %d\n", totalQueries);
        printf("Benchmark time: %ld ms\n", totalDuration);
        printf("Avg latency: %f ms\n", (double) totalDuration / totalQueries);
        printf("Total queries skipped: %d\n", totalQueriesSkipped);
        printf("Recall: %f\n", (double) recall / (queryNumVectors * k));
    }
    return 0;
}
