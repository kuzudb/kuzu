#include "src/testing/include/test_helper.h"

#include <filesystem>

using namespace std;
using namespace graphflow::runner;
using namespace graphflow::planner;

namespace graphflow {
namespace testing {

bool TestHelper::runTest(const string& path) {
    auto testConfig = parseTestFile(path);
    if (!filesystem::create_directory(testConfig.graphOutputDir)) {
        throw invalid_argument(
            "Graph output directory cannot be created. Check if it exists and remove it.");
    }
    auto server = make_unique<EmbeddedServer>(testConfig.graphInputDir, testConfig.graphOutputDir,
        testConfig.numThreads, testConfig.bufferPoolSize);
    auto numQueries = testConfig.query.size();
    uint64_t numPassedQueries = 0;
    vector<uint64_t> numPlansOfEachQuery(numQueries);
    vector<uint64_t> numPassedPlansOfEachQuery(numQueries);
    for (uint64_t i = 0; i < numQueries; i++) {
        spdlog::info("TEST: {}", testConfig.name[i]);
        spdlog::info("QUERY: {}", testConfig.query[i]);
        auto plans = server->enumerateLogicalPlans(testConfig.query[i]);
        auto numPlans = plans.size();
        numPlansOfEachQuery[i] = numPlans;
        uint64_t numPassedPlans = 0;
        for (uint64_t j = 0; j < numPlans; j++) {
            auto planStr = plans[j]->getLastOperator().toString();
            auto result = server->execute(move(plans[j]));
            if (result->numTuples != testConfig.expectedNumTuples[i]) {
                spdlog::error("PLAN{} NOT PASSED. Result num tuples: {}, Expected num tuples: {}",
                    j, result->numTuples, testConfig.expectedNumTuples[i]);
                spdlog::info("PLAN: \n{}", planStr);
            } else {
                if (testConfig.compareResult) {
                    vector<string> resultTuples;
                    for (auto& tuple : result->tuples) {
                        for (uint64_t k = 0; k < tuple.multiplicity; k++) {
                            resultTuples.push_back(tuple.toString());
                        }
                    }
                    sort(resultTuples.begin(), resultTuples.end());
                    if (resultTuples == testConfig.expectedTuples[i]) {
                        spdlog::info("PLAN{} PASSED", j);
                        spdlog::debug("PLAN: \n{}", planStr);
                        numPassedPlans++;
                    } else {
                        spdlog::error("PLAN{} NOT PASSED. Result tuples are not matched", j);
                        spdlog::info("PLAN: \n{}", planStr);
                        spdlog::info("RESULT: \n");
                        for (auto& tuple : resultTuples) {
                            spdlog::info(tuple);
                        }
                    }
                } else {
                    spdlog::info("PLAN{} PASSED", j);
                    spdlog::debug("PLAN: \n{}", planStr);
                    numPassedPlans++;
                }
            }
        }
        numPassedPlansOfEachQuery[i] = numPassedPlans;
        spdlog::info("{}/{} plans passed in this query.", numPassedPlans, numPlans);
    }
    spdlog::info("SUMMARY:");
    for (uint64_t i = 0; i < numQueries; i++) {
        spdlog::info("{}: {}/{} PLANS PASSED", testConfig.name[i], numPassedPlansOfEachQuery[i],
            numPlansOfEachQuery[i]);
        numPassedQueries += (numPlansOfEachQuery[i] == numPassedPlansOfEachQuery[i]);
    }
    error_code removeErrorCode;
    if (!filesystem::remove_all(testConfig.graphOutputDir, removeErrorCode)) {
        spdlog::error("Remove graph output directory error: {}", removeErrorCode.message());
    }
    return numPassedQueries == numQueries;
}

bool TestHelper::runExceptionTest(const string& path) {
    auto testConfig = parseTestFile(path);
    if (!filesystem::create_directory(testConfig.graphOutputDir)) {
        throw invalid_argument(
            "Graph output directory cannot be created. Check if it exists and remove it.");
    }
    auto server = make_unique<EmbeddedServer>(testConfig.graphInputDir, testConfig.graphOutputDir,
        testConfig.numThreads, testConfig.bufferPoolSize);
    auto numQueries = testConfig.query.size();
    uint64_t numPassedQueries = 0;
    for (auto i = 0u; i < numQueries; i++) {
        spdlog::info("TEST: {}", testConfig.name[i]);
        spdlog::info("QUERY: {}", testConfig.query[i]);
        try {
            auto plans = server->enumerateLogicalPlans(testConfig.query[i]);
            spdlog::error(
                "QUERY: {} NOT PASSED. Expect malformed to be thrown.", testConfig.query[i]);
        } catch (const invalid_argument& exception) {
            if (testConfig.expectedErrorMsgs[i] != exception.what()) {
                spdlog::error("QUERY: {} NOT PASSED. Expect error message {} but get {}.",
                    testConfig.query[i], testConfig.expectedErrorMsgs[i], exception.what());
            } else {
                numPassedQueries++;
            }
        }
    }
    error_code removeErrorCode;
    if (!filesystem::remove_all(testConfig.graphOutputDir, removeErrorCode)) {
        spdlog::error("Remove graph output directory error: {}", removeErrorCode.message());
    }
    return numPassedQueries == numQueries;
}

TestSuiteConfig TestHelper::parseTestFile(const string& path) {
    if (access(path.c_str(), 0) == 0) {
        struct stat status;
        stat(path.c_str(), &status);
        if (!(status.st_mode & S_IFDIR)) {
            ifstream ifs(path);
            string line;
            TestSuiteConfig config;
            while (getline(ifs, line)) {
                if (line.starts_with("-INPUT")) {
                    config.graphInputDir = line.substr(7, line.length());
                } else if (line.starts_with("-OUTPUT")) {
                    config.graphOutputDir = line.substr(8, line.length());
                } else if (line.starts_with("-PARALLELISM")) {
                    config.numThreads = stoi(line.substr(13, line.length()));
                } else if (line.starts_with("-BUFFER_POOL_SIZE")) {
                    config.bufferPoolSize = stoi(line.substr(18, line.length()));
                } else if (line.starts_with("-COMPARE_RESULT")) {
                    config.compareResult = (line.substr(16, line.length()) == "1");
                } else if (line.starts_with("-NAME")) {
                    config.name.push_back(line.substr(6, line.length()));
                } else if (line.starts_with("-QUERY")) {
                    config.query.push_back(line.substr(7, line.length()));
                } else if (line.starts_with("-EXCEPTION")) {
                    config.expectedErrorMsgs.push_back(line.substr(11, line.length()));
                } else if (line.starts_with("----")) {
                    uint64_t numTuples = stoi(line.substr(5, line.length()));
                    config.expectedNumTuples.push_back(numTuples);
                    vector<string> queryExpectedTuples;
                    if (config.compareResult) {
                        for (uint64_t i = 0; i < numTuples; i++) {
                            getline(ifs, line);
                            queryExpectedTuples.push_back(line);
                        }
                    }
                    sort(queryExpectedTuples.begin(), queryExpectedTuples.end());
                    config.expectedTuples.push_back(queryExpectedTuples);
                }
            }
            return config;
        }
    }
    throw invalid_argument("Test file not exists! [" + path + "].");
}
} // namespace testing
} // namespace graphflow
