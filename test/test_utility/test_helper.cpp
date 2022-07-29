#include "test/test_utility/include/test_helper.h"

#include <fstream>

#include "spdlog/spdlog.h"

#include "src/loader/include/graph_loader.h"

using namespace std;
using namespace graphflow::planner;
using namespace graphflow::loader;

namespace graphflow {
namespace testing {

vector<TestQueryConfig> TestHelper::parseTestFile(const string& path, bool checkOutputOrder) {
    vector<TestQueryConfig> retVal;
    if (access(path.c_str(), 0) == 0) {
        struct stat status;
        stat(path.c_str(), &status);
        if (!(status.st_mode & S_IFDIR)) {
            ifstream ifs(path);
            string line;
            while (getline(ifs, line)) {
                if (line.starts_with("-NAME")) {
                    // We create a new TestConfig when we see the line with -NAME. Following lines
                    // until the next -NAME configure this new test.
                    TestQueryConfig newConfig;
                    retVal.push_back(newConfig);
                    retVal.back().name = line.substr(6, line.length());
                } else if (line.starts_with("-PARALLELISM")) {
                    retVal.back().numThreads = stoi(line.substr(13, line.length()));
                } else if (line.starts_with("-QUERY")) {
                    retVal.back().query = line.substr(7, line.length());
                } else if (line.starts_with("-ENCODED_JOIN")) {
                    retVal.back().encodedJoin = line.substr(14, line.length());
                } else if (line.starts_with("----")) {
                    uint64_t numTuples = stoi(line.substr(5, line.length()));
                    retVal.back().expectedNumTuples = numTuples;
                    for (auto i = 0u; i < numTuples; i++) {
                        getline(ifs, line);
                        retVal.back().expectedTuples.push_back(line);
                    }
                    if (!checkOutputOrder) {
                        sort(retVal.back().expectedTuples.begin(),
                            retVal.back().expectedTuples.end());
                    }
                }
            }
            return retVal;
        }
    }
    throw invalid_argument("Test file not exists! [" + path + "].");
}

bool TestHelper::runTest(const vector<TestQueryConfig>& testConfigs, Connection& conn) {
    auto numQueries = testConfigs.size();
    uint64_t numPassedQueries = 0;
    vector<uint64_t> numPlansOfEachQuery(numQueries);
    vector<uint64_t> numPassedPlansOfEachQuery(numQueries);
    for (uint64_t i = 0; i < numQueries; i++) {
        auto testConfig = testConfigs[i];
        spdlog::info("TEST: {}", testConfig.name);
        spdlog::info("QUERY: {}", testConfig.query);
        conn.setMaxNumThreadForExec(testConfig.numThreads);
        auto plans = conn.enumeratePlans(testConfig.query);
        auto numPlans = plans.size();
        assert(numPlans > 0);
        numPlansOfEachQuery[i] = numPlans;
        uint64_t numPassedPlans = 0;
        for (uint64_t j = 0; j < numPlans; j++) {
            auto planStr = plans[j]->getLastOperator()->toString();
            auto result = conn.executePlan(move(plans[j]));
            assert(result->isSuccess());
            auto numTuples = result->getNumTuples();
            if (numTuples != testConfig.expectedNumTuples) {
                spdlog::error("PLAN{} NOT PASSED. Result num tuples: {}, Expected num tuples: {}",
                    j, numTuples, testConfig.expectedNumTuples);
                spdlog::info("PLAN: \n{}", planStr);
            } else {
                vector<string> resultTuples =
                    convertResultToString(*result, testConfig.checkOutputOrder);
                if (resultTuples == testConfig.expectedTuples) {
                    spdlog::info(
                        "PLAN{} PASSED in {}ms", j, result->getQuerySummary()->getExecutionTime());
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
            }
        }
        numPassedPlansOfEachQuery[i] = numPassedPlans;
        spdlog::info("{}/{} plans passed in this query.", numPassedPlans, numPlans);
    }
    spdlog::info("SUMMARY:");
    for (uint64_t i = 0; i < numQueries; i++) {
        spdlog::info("{}: {}/{} PLANS PASSED", testConfigs[i].name, numPassedPlansOfEachQuery[i],
            numPlansOfEachQuery[i]);
        numPassedQueries += (numPlansOfEachQuery[i] == numPassedPlansOfEachQuery[i]);
    }
    return numPassedQueries == numQueries;
}

vector<string> TestHelper::convertResultToString(QueryResult& queryResult, bool checkOutputOrder) {
    vector<string> actualOutput;
    while (queryResult.hasNext()) {
        auto tuple = queryResult.getNext();
        actualOutput.push_back(tuple->toString(vector<uint32_t>(tuple->len(), 0)));
    }
    if (!checkOutputOrder) {
        sort(actualOutput.begin(), actualOutput.end());
    }
    return actualOutput;
}

void BaseGraphLoadingTest::loadGraph() {
    GraphLoader graphLoader(getInputCSVDir(), TestHelper::TEMP_TEST_DIR,
        systemConfig->maxNumThreads, systemConfig->defaultPageBufferPoolSize,
        systemConfig->largePageBufferPoolSize);
    graphLoader.loadGraph();
}

void BaseGraphLoadingTest::createDBAndConn() {
    database = make_unique<Database>(*databaseConfig, *systemConfig);
    conn = make_unique<Connection>(database.get());
}

} // namespace testing
} // namespace graphflow
