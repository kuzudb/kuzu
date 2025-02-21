#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>
#include "assert.h"
#include <sys/stat.h>
#include <chrono>
#include <algorithm>
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

std::vector<std::string> readQueriesFromFile(const std::string &filePath, int efSearch, int maxK, bool useQ, bool useKnn, std::string &searchType) {
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

        // Replace <useQ> with the value of useQ
        while ((pos = line.find("<useQ>")) != std::string::npos) {
            line.replace(pos, 6, useQ ? "true" : "false");
        }

        // Replace <knn> with the value of useKnn
        while ((pos = line.find("<knn>")) != std::string::npos) {
            line.replace(pos, 5, useKnn ? "true" : "false");
        }

        // Replace <searchType> with the value of searchType
        while ((pos = line.find("<searchType>")) != std::string::npos) {
            line.replace(pos, 12, "'" + searchType + "'");
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

/*
 * measureRecall:
 *
 * Runs the queries (read from the query file with substitutions using the candidate efSearch)
 * and computes the recall percentage over up to nQueries queries.
 *
 * The ground-truth file (gtPath) is loaded, and for each query the number of ground-truth
 * matches (up to k per query) is counted. The recall percentage is then computed as:
 *
 *      recall% = (total matches / (number of valid queries * k)) * 100.0
 *
 * If a query returns fewer than k tuples, it is skipped.
 */
double measureRecall(Connection &conn, const std::string &queriesPath, int efSearch, int maxK,
                     bool useQ, bool useKnn, const std::string &searchType,
                     const std::string &gtPath, int nQueries, int k) {
    auto queries = readQueriesFromFile(queriesPath, efSearch, maxK, useQ, useKnn, const_cast<std::string&>(searchType));
    int queryNumVectors = queries.size();
    if (nQueries < queryNumVectors) {
        queryNumVectors = nQueries;
    }
    vector_id_t *gtVecs = new vector_id_t[queryNumVectors * k];
    loadFromFile(gtPath, reinterpret_cast<uint8_t *>(gtVecs), queryNumVectors * k * sizeof(vector_id_t));
    int totalRecall = 0;
    int validQueries = 0;

    for (int i = 0; i < queryNumVectors; i++) {
        auto res = conn.query(queries[i]);
        // If fewer than k tuples are returned, skip this query.
        if (res->getNumTuples() < k) {
            continue;
        }
        validQueries++;
        while (res->hasNext()) {
            auto row = res->getNext();
            auto val = row->getValue(0)->getValue<int64_t>();
            if (std::find(gtVecs + i * k, gtVecs + (i + 1) * k, val) != (gtVecs + (i + 1) * k)) {
                totalRecall++;
            }
        }
    }
    delete[] gtVecs;
    double recallPercentage = 0.0;
    if (validQueries > 0)
        recallPercentage = (double) totalRecall / (validQueries * k) * 100.0;
    return recallPercentage;
}

int
printCommands(std::string &databasePath, std::string &queriesPath, std::string &gtPath, int warmup, int nQueries, int k,
              int efSearch, int maxK, bool useQ, bool useKnn, std::string &searchType, int maxNumThreads,
              std::string &selectivityStr, std::string &outputDir) {
    printf("${SLURM_TMPDIR}/kuzu/build/release/tools/benchmark_vector/kuzu_benchmark_vector -databasePath %s -queriesPath %s -gtPath %s -warmup %d -k %d -efSearch %d -maxK %d -useQ %d -useKnn %d -nQueries %d -maxNumThreads %d -searchType %s -selectivity %s -outputDir %s\n",
           databasePath.c_str(), queriesPath.c_str(), gtPath.c_str(), warmup, k, efSearch, maxK, useQ, useKnn, nQueries,
           maxNumThreads, searchType.c_str(), selectivityStr.c_str(), outputDir.c_str());
    return 0;
}

int main(int argc, char **argv) {
    InputParser input(argc, argv);
    std::string databasePath = input.getCmdOption("-databasePath");
    std::string queryPath = input.getCmdOption("-queriesPath");
    std::string gtPath = input.getCmdOption("-gtPath");
    int warmupTimes = std::stoi(input.getCmdOption("-warmup"));
    int nQueries = std::stoi(input.getCmdOption("-nQueries"));
    int k = std::stoi(input.getCmdOption("-k"));
    int efSearch = 0;  // Will be set either from command-line or via binary search.
    bool binarySearchMode = false;

    // Check if binary search mode parameters are provided.
    // (Both -minRecall and -maxRecall must be provided to trigger binary search.)
    std::string minRecallStr = input.getCmdOption("-minRecall");
    std::string maxRecallStr = input.getCmdOption("-maxRecall");

    if (!minRecallStr.empty() && !maxRecallStr.empty()) {
        binarySearchMode = true;
    } else {
        // Standard mode: use the given efSearch value.
        efSearch = std::stoi(input.getCmdOption("-efSearch"));
    }

    // New: get output file for JSON results (only used in standard mode)
    std::string outputDir = input.getCmdOption("-outputDir");

    int maxK = std::stoi(input.getCmdOption("-maxK"));
    bool useQ = (bool) std::stoi(input.getCmdOption("-useQ"));
    bool useKnn = (bool) std::stoi(input.getCmdOption("-useKnn"));
    std::string searchTypeStr = input.getCmdOption("-searchType");
    std::string selectivityStr = input.getCmdOption("-selectivity");
    std::string maxNumThreadsStr = input.getCmdOption("-maxNumThreads");
    int maxNumThreads = 32;
    if (!maxNumThreadsStr.empty()) {
        maxNumThreads = std::stoi(maxNumThreadsStr);
    }

    std::string outputFile = "";
    if (!outputDir.empty()) {
        outputFile = outputDir + "/output_" + selectivityStr + "_" + searchTypeStr + ".json";
    }

    auto db = Database(databasePath);
    auto conn = Connection(&db);
    printf("# Max num threads: %d\n", maxNumThreads);
    conn.setMaxNumThreadForExec(maxNumThreads);

    // Binary search mode branch (unchanged output)
    if (binarySearchMode) {
        double minRecall = std::stod(minRecallStr);  // e.g., 95.0 for 95%
        double maxRecall = std::stod(maxRecallStr);
        int lowEf = 100;
        int highEf = 1000;
        int bestEfSearch = -1;
        double bestRecall = 0.0;

        // Try lowEf and highEf first.
        double lowRecall = measureRecall(conn, queryPath, lowEf, maxK, useQ, useKnn, searchTypeStr, gtPath, nQueries, k);
        if (lowRecall >= minRecall) {
            printf("# Binary Search: Trying efSearch = %d, recall = %.2f%%\n", lowEf, lowRecall);
            bestEfSearch = lowEf;
            bestRecall = lowRecall;
            printCommands(databasePath, queryPath, gtPath, warmupTimes, nQueries, k, bestEfSearch, maxK, useQ, useKnn, searchTypeStr, maxNumThreads, selectivityStr, outputDir);
            return 0;
        }

        double highRecall = measureRecall(conn, queryPath, highEf, maxK, useQ, useKnn, searchTypeStr, gtPath, nQueries, k);
        if (highRecall <= maxRecall) {
            printf("# Binary Search: Trying efSearch = %d, recall = %.2f%%\n", highEf, highRecall);
            bestEfSearch = highEf;
            bestRecall = highRecall;
            printCommands(databasePath, queryPath, gtPath, warmupTimes, nQueries, k, bestEfSearch, maxK, useQ, useKnn, searchTypeStr, maxNumThreads, selectivityStr, outputDir);
            return 0;
        }

        // Binary search loop with candidate values as multiples of 5.
        while (lowEf <= highEf) {
            int midEf = (lowEf + highEf) / 2;
            // Ensure midEf is a multiple of 5.
            midEf = (midEf / 5) * 5;
            if (midEf < lowEf) {
                midEf = lowEf;
            }
            if (midEf > highEf) {
                midEf = highEf;
            }
            double currentRecall = measureRecall(conn, queryPath, midEf, maxK, useQ, useKnn, searchTypeStr, gtPath, nQueries, k);
            printf("# Binary Search: Trying efSearch = %d, recall = %.2f%%\n", midEf, currentRecall);

            if (currentRecall < minRecall) {
                lowEf = midEf + 5;
            } else if (currentRecall > maxRecall) {
                highEf = midEf - 5;
            } else {  // currentRecall is within acceptable bounds.
                bestEfSearch = midEf;
                bestRecall = currentRecall;
                // Try to see if a lower multiple of 5 still works.
                highEf = midEf - 5;
            }
        }

        if (bestEfSearch == -1) {
            bestEfSearch = lowEf;
        }
        efSearch = bestEfSearch;
        printf("# Optimal efSearch found: %d with recall: %.2f%%\n", efSearch, bestRecall);
        printCommands(databasePath, queryPath, gtPath, warmupTimes, nQueries, k, bestEfSearch, maxK, useQ, useKnn, searchTypeStr, maxNumThreads, selectivityStr, outputDir);
        return 0;
    }

    // Standard benchmark mode (running with an explicit efSearch value)
    std::vector<std::string> testQueries;
    testQueries.push_back(queryPath);

    // Metrics that will be aggregated from the benchmark run.
    int totalQueries = 0;
    int totalQueriesSkipped = 0;
    int recallCount = 0;
    long executionTime = 0;
    long compilationTime = 0;
    double vectorSearchTime = 0;
    long avgDistanceComputations = 0;
    double distCompTime = 0;
    long listNbrsCalls = 0;
    double listNbrsCallsTime = 0;
    long oneHopCalls = 0;
    long twoHopCalls = 0;
    long dynamicTwoHopCalls = 0;

    for (auto &queriesPath : testQueries) {
        printf("Running queries from: %s\n", queriesPath.c_str());
        auto queries = readQueriesFromFile(queriesPath, efSearch, maxK, useQ, useKnn, searchTypeStr);
        int queryNumVectors = queries.size();
        if (nQueries < queryNumVectors) {
            queryNumVectors = nQueries;
        }

        vector_id_t *gtVecs = new vector_id_t[queryNumVectors * k];
        loadFromFile(gtPath, reinterpret_cast<uint8_t *>(gtVecs), queryNumVectors * k * sizeof(vector_id_t));

        // Warmup phase.
        for (int i = 0; i < warmupTimes; i++) {
            printf("Warmup started %d\n", i);
            auto start = std::chrono::high_resolution_clock::now();
            for (int j = 0; j < queryNumVectors; j++) {
                conn.query(queries[j]);
            }
            auto end = std::chrono::high_resolution_clock::now();
            auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
            printf("Warmup finished %d\n", i);
            printf("Warmup time %d: %lld ms\n", i, duration);
        }

        // Benchmark phase.
        printf("Benchmark started\n");

        for (int i = 0; i < queryNumVectors; i++) {
            printf("====== Running query %d ======\n", i);
            int localRecall = 0;
            auto res = conn.query(queries[i]);
            res = conn.query(queries[i]);  // Second run (if desired)
            long duration = res->getQuerySummary()->getExecutionTime();
            executionTime += duration;
            compilationTime += res->getQuerySummary()->getCompilingTime();
            auto vectorStats = res->getQuerySummary()->getVectorSearchSummary();
            vectorSearchTime += vectorStats.vectorSearchTime;
            avgDistanceComputations += vectorStats.distanceComputations;
            distCompTime += vectorStats.distanceComputationsTime;
            listNbrsCalls += vectorStats.listNbrsCalls;
            listNbrsCallsTime += vectorStats.listNbrsCallsTime;
            oneHopCalls += vectorStats.oneHopCalls;
            twoHopCalls += vectorStats.twoHopCalls;
            dynamicTwoHopCalls += vectorStats.dynamicTwoHopCalls;

            if (res->getNumTuples() < k) {
                totalQueriesSkipped++;
                printf("Skipped query %d\n", i);
                continue;
            }
            totalQueries++;
            vector_id_t *gt = gtVecs + i * k;
            while (res->hasNext()) {
                auto row = res->getNext();
                auto val = row->getValue(0)->getValue<int64_t>();
                if (std::find(gt, gt + k, val) != (gt + k)) {
                    recallCount++;
                    localRecall++;
                }
            }
            printf("Recall for query %d: %d\n", i, localRecall);
            printf("Duration: %ld ms\n", duration);
            printf("====================================\n");
        }
        printf("Benchmark finished\n");

        // Release memory for gt vectors.
        delete[] gtVecs;
    }

    // Compute averages (guarding against division by zero)
    double avgExecTime = totalQueries > 0 ? (double) executionTime / totalQueries : 0;
    double avgCompTime = totalQueries > 0 ? (double) compilationTime / totalQueries : 0;
    double avgVectorSearchTime = totalQueries > 0 ? (double) vectorSearchTime / totalQueries : 0;
    double avgDistComputations = totalQueries > 0 ? (double) avgDistanceComputations / totalQueries : 0;
    double avgDistComputationsTime = totalQueries > 0 ? (double) distCompTime / totalQueries : 0;
    double avgListNbrsCalls = totalQueries > 0 ? (long ) listNbrsCalls / totalQueries : 0;
    double avgListNbrsCallsTime = totalQueries > 0 ? (double) listNbrsCallsTime / totalQueries : 0;
    double avgOneHopCalls = totalQueries > 0 ? (double) oneHopCalls / totalQueries : 0;
    double avgTwoHopCalls = totalQueries > 0 ? (double) twoHopCalls / totalQueries : 0;
    double avgDynamicTwoHopCalls = totalQueries > 0 ? (double) dynamicTwoHopCalls / totalQueries : 0;
    double recallPercentage = (double) recallCount / (nQueries * k) * 100.0;

    // Build JSON output for benchmark summary.
    std::ostringstream jsonStream;
    jsonStream << "{\n";
    jsonStream << "  \"total_queries\": " << totalQueries << ",\n";
    jsonStream << "  \"avg_execution_time_ms\": " << avgExecTime << ",\n";
    jsonStream << "  \"avg_compilation_time_ms\": " << avgCompTime << ",\n";
    jsonStream << "  \"avg_vector_search_time_ms\": " << avgVectorSearchTime << ",\n";
    jsonStream << "  \"avg_distance_computations\": " << avgDistComputations << ",\n";
    jsonStream << "  \"avg_distance_computations_time_ms\": " << avgDistComputationsTime << ",\n";
    jsonStream << "  \"avg_list_nbrs_calls\": " << avgListNbrsCalls << ",\n";
    jsonStream << "  \"avg_list_nbrs_calls_time_ms\": " << avgListNbrsCallsTime << ",\n";
    jsonStream << "  \"avg_one_hop_calls\": " << avgOneHopCalls << ",\n";
    jsonStream << "  \"avg_two_hop_calls\": " << avgTwoHopCalls << ",\n";
    jsonStream << "  \"avg_dynamic_two_hop_calls\": " << avgDynamicTwoHopCalls << ",\n";
    jsonStream << "  \"total_queries_skipped\": " << totalQueriesSkipped << ",\n";
    jsonStream << "  \"recall_percentage\": " << recallPercentage << "\n";
    jsonStream << "  \"selectivity\": " << stoi(selectivityStr) << "\n";
    jsonStream << "  \"efSearch\": " << efSearch << "\n";
    jsonStream << "}\n";

    // Write JSON output to file if provided.
    if (!outputFile.empty()) {
        std::ofstream outFile(outputFile);
        if (outFile.is_open()) {
            outFile << jsonStream.str();
            outFile.close();
            printf("Benchmark summary written to %s\n", outputFile.c_str());
        } else {
            std::cerr << "Error opening output file: " << outputFile << std::endl;
        }
    } else {
        // If no output file was provided, print JSON to stdout.
        std::cout << jsonStream.str() << std::endl;
    }

    return 0;
}
