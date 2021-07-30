#include "test/test_utility/include/test_helper.h"

#include <filesystem>
#include <fstream>

#include "spdlog/spdlog.h"

#include "src/loader/include/graph_loader.h"
#include "src/processor/include/physical_plan/operator/result/result_set_iterator.h"

using namespace std;
using namespace graphflow::planner;
using namespace graphflow::loader;

namespace graphflow {
namespace testing {

bool TestHelper::runTest(const TestSuiteQueryConfig& testConfig, const System& system) {
    SessionContext context;
    auto numQueries = testConfig.query.size();
    uint64_t numPassedQueries = 0;
    vector<uint64_t> numPlansOfEachQuery(numQueries);
    vector<uint64_t> numPassedPlansOfEachQuery(numQueries);
    for (uint64_t i = 0; i < numQueries; i++) {
        spdlog::info("TEST: {}", testConfig.name[i]);
        spdlog::info("QUERY: {}", testConfig.query[i]);
        context.query = testConfig.query[i];
        auto plans = system.enumerateAllPlans(context);
        auto numPlans = plans.size();
        assert(numPlans > 0);
        numPlansOfEachQuery[i] = numPlans;
        uint64_t numPassedPlans = 0;
        for (uint64_t j = 0; j < numPlans; j++) {
            auto planStr = plans[j]->lastOperator->toString();
            auto result = system.executePlan(move(plans[j]), context);
            if (result->numTuples != testConfig.expectedNumTuples[i]) {
                spdlog::error("PLAN{} NOT PASSED. Result num tuples: {}, Expected num tuples: {}",
                    j, result->numTuples, testConfig.expectedNumTuples[i]);
                spdlog::info("PLAN: \n{}", planStr);
            } else {
                if (testConfig.compareResult) {
                    vector<string> resultTuples;
                    auto resultSetIterator =
                        make_unique<ResultSetIterator>(result->resultSetCollection[0].get());
                    Tuple tuple(resultSetIterator->dataTypes);
                    for (auto& resultSet : result->resultSetCollection) {
                        resultSetIterator->setResultSet(resultSet.get());
                        while (resultSetIterator->hasNextTuple()) {
                            resultSetIterator->getNextTuple(tuple);
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
    return numPassedQueries == numQueries;
}

unique_ptr<TestSuiteQueryConfig> TestHelper::parseTestFile(const string& path) {
    if (access(path.c_str(), 0) == 0) {
        struct stat status;
        stat(path.c_str(), &status);
        if (!(status.st_mode & S_IFDIR)) {
            ifstream ifs(path);
            string line;
            auto config = make_unique<TestSuiteQueryConfig>();
            while (getline(ifs, line)) {
                if (line.starts_with("-PARALLELISM")) {
                    config->numThreads = stoi(line.substr(13, line.length()));
                } else if (line.starts_with("-COMPARE_RESULT")) {
                    config->compareResult = (line.substr(16, line.length()) == "1");
                } else if (line.starts_with("-NAME")) {
                    config->name.push_back(line.substr(6, line.length()));
                } else if (line.starts_with("-QUERY")) {
                    config->query.push_back(line.substr(7, line.length()));
                } else if (line.starts_with("----")) {
                    uint64_t numTuples = stoi(line.substr(5, line.length()));
                    config->expectedNumTuples.push_back(numTuples);
                    vector<string> queryExpectedTuples;
                    if (config->compareResult) {
                        for (auto i = 0u; i < numTuples; i++) {
                            getline(ifs, line);
                            queryExpectedTuples.push_back(line);
                        }
                    }
                    sort(queryExpectedTuples.begin(), queryExpectedTuples.end());
                    config->expectedTuples.push_back(queryExpectedTuples);
                }
            }
            return config;
        }
    }
    throw invalid_argument("Test file not exists! [" + path + "].");
}

void TestHelper::loadGraph(TestSuiteSystemConfig& config) {
    GraphLoader graphLoader(config.graphInputDir, config.graphOutputDir, config.maxNumThreads);
    graphLoader.loadGraph();
}

void BaseGraphLoadingTest::SetUp() {
    testSuiteSystemConfig.graphInputDir = getInputCSVDir();
    testSuiteSystemConfig.graphOutputDir = TEMP_TEST_DIR;
    TestHelper::loadGraph(testSuiteSystemConfig);
}

} // namespace testing
} // namespace graphflow
