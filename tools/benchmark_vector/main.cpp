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

std::vector<std::string> readQueriesFromFile(const std::string &filePath, int efSearch, int maxK) {
    std::ifstream inputFile(filePath);
    std::vector<std::string> queries;

    if (!inputFile.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        return queries; // Return empty vector on error
    }

    std::string line;
    while (std::getline(inputFile, line)) {
        // Replace <efsearch> with the value of efSearch
        size_t pos;
        while ((pos = line.find("<efsearch>")) != std::string::npos) {
            line.replace(pos, 10, std::to_string(efSearch));
        }

        // Replace <maxK> with the value of maxK
        while ((pos = line.find("<maxK>")) != std::string::npos) {
            line.replace(pos, 6, std::to_string(maxK));
        }

        queries.push_back(line); // Push the modified line as a query
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
    int efSearch = stoi(input.getCmdOption("-efSearch"));
    int maxK = stoi(input.getCmdOption("-maxK"));

    std::vector<std::string> testQueries;
    testQueries.push_back(queryPath);

    auto db = Database(databasePath);
    auto conn = Connection(&db);

    for (auto &queriesPath: testQueries) {
        printf("Running queries from: %s\n", queriesPath.c_str());
        auto queries = readQueriesFromFile(queriesPath, efSearch, maxK);
        auto queryNumVectors = queries.size();
        vector_id_t *gtVecs = new vector_id_t[queryNumVectors * k];
        loadFromFile(gtPath, reinterpret_cast<uint8_t *>(gtVecs), queryNumVectors * k * sizeof(vector_id_t));
        // Randomly select warmupTimes queries from the queries
        for (int i = 0; i < warmupTimes; i++) {
            printf("Warmup started %d\n", i);
            auto start = std::chrono::high_resolution_clock::now();
            for (int j = 0; j < queries.size(); j++) {
                conn.query(queries[j]);
            }
            // Get the current time after the function call
            auto end = std::chrono::high_resolution_clock::now();
            // Calculate the duration of the function execution
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            printf("Warmup finished\n");
            printf("Warmup time %d: %lld ms\n", i, duration);
        }
        // Run all queries and print avg latency
        printf("Benchmark started\n");
        auto recall = 0;
        auto totalQueries = 0;
        auto totalQueriesSkipped = 0;
        long executionTime = 0;
        long compilationTime = 0;
        long vectorSearchTime = 0;
        long avgDistanceComputations = 0;
        long avgListNbrsCalls = 0;
        for (auto i = 0; i < queries.size(); i++) {
            printf("====== running query %d ======\n", i);
            auto localRecall = 0;
            auto res = conn.query(queries[i]);
            res = conn.query(queries[i]);
            long duration = res->getQuerySummary()->getExecutionTime();
            executionTime += duration;
            compilationTime += res->getQuerySummary()->getCompilingTime();
            vectorSearchTime += res->getQuerySummary()->getVectorSearchTime();
            avgDistanceComputations += res->getQuerySummary()->getDistanceComputations();
            avgListNbrsCalls += res->getQuerySummary()->getListNbrsCalls();
            if (res->getNumTuples() < k) {
                totalQueriesSkipped += 1;
                printf("skipped query %d\n", i);
                continue;
            }
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
            printf("distance computations: %d, list nbrs calls: %d\n",
                   res->getQuerySummary()->getDistanceComputations(), res->getQuerySummary()->getListNbrsCalls());
            printf("====================================\n");
            totalQueries++;
        }
        printf("Benchmark finished\n");
        printf("Total queries: %d\n", totalQueries);
        printf("Avg execution time: %f ms\n", (double) executionTime / totalQueries);
        printf("Avg compilation time: %f ms\n", (double) compilationTime / totalQueries);
        printf("Avg vector search time: %f ms\n", (double) vectorSearchTime / totalQueries);
        printf("Avg distance computations: %f\n", (double) avgDistanceComputations / totalQueries);
        printf("Avg list nbrs calls: %f\n", (double) avgListNbrsCalls / totalQueries);
        printf("Total queries skipped: %d\n", totalQueriesSkipped);
        printf("Recall: %f\n", (double) recall / (queryNumVectors * k));

        // release memory
        delete[] gtVecs;
    }
    return 0;
}