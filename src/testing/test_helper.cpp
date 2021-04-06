#include "src/testing/include/test_helper.h"

#include <filesystem>

using namespace std;
using namespace graphflow::runner;
using namespace graphflow::planner;

namespace graphflow {
namespace testing {

const string TEST_TEMP_DIR = "test_temp";

bool TestHelper::runTest(const string& path) {
    auto testConfig = parseTestFile(path);
    filesystem::create_directory(TEST_TEMP_DIR);
    auto server = make_unique<EmbeddedServer>(
        testConfig.graphInputDir, TEST_TEMP_DIR, testConfig.numThreads, testConfig.bufferPoolSize);
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
                spdlog::info("PLAN{} PASSED", j);
                spdlog::debug("PLAN: \n{}", planStr);
                numPassedPlans++;
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
                } else if (line.starts_with("-PARALLELISM")) {
                    config.numThreads = stoi(line.substr(13, line.length()));
                } else if (line.starts_with("-BUFFER_POOL_SIZE")) {
                    config.bufferPoolSize = stoi(line.substr(18, line.length()));
                } else if (line.starts_with("-NAME")) {
                    config.name.push_back(line.substr(6, line.length()));
                } else if (line.starts_with("-QUERY")) {
                    config.query.push_back(line.substr(7, line.length()));
                } else if (line.starts_with("----")) {
                    config.expectedNumTuples.push_back(stoi(line.substr(5, line.length())));
                }
            }
            return config;
        }
    }
    throw invalid_argument("Test file not exists! [" + path + "].");
}
} // namespace testing
} // namespace graphflow
