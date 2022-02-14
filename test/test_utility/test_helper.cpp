#include "test/test_utility/include/test_helper.h"

#include <fstream>

#include "spdlog/spdlog.h"

#include "src/loader/include/graph_loader.h"

using namespace std;
using namespace graphflow::planner;
using namespace graphflow::loader;

namespace graphflow {
namespace testing {

bool TestHelper::runTest(const vector<TestQueryConfig>& testConfigs, const System& system) {
    SessionContext context;
    auto numQueries = testConfigs.size();
    uint64_t numPassedQueries = 0;
    vector<uint64_t> numPlansOfEachQuery(numQueries);
    vector<uint64_t> numPassedPlansOfEachQuery(numQueries);
    for (uint64_t i = 0; i < numQueries; i++) {
        auto testConfig = testConfigs[i];
        spdlog::info("TEST: {}", testConfig.name);
        spdlog::info("QUERY: {}", testConfig.query);
        context.query = testConfig.query;
        context.numThreads = testConfig.numThreads;
        auto plans = system.enumerateAllPlans(context);
        auto numPlans = plans.size();
        assert(numPlans > 0);
        numPlansOfEachQuery[i] = numPlans;
        uint64_t numPassedPlans = 0;
        vector<DataType> dataTypes = plans[0]->getExpressionsToCollectDataTypes();
        for (uint64_t j = 0; j < numPlans; j++) {
            auto planStr = plans[j]->lastOperator->toString();
            auto result = system.executePlan(move(plans[j]), context);
            if (result->getTotalNumFlatTuples() != testConfig.expectedNumTuples) {
                spdlog::error("PLAN{} NOT PASSED. Result num tuples: {}, Expected num tuples: {}",
                    j, result->getTotalNumFlatTuples(), testConfig.expectedNumTuples);
                spdlog::info("PLAN: \n{}", planStr);
            } else {
                vector<string> resultTuples =
                    getActualOutput(*result, dataTypes, testConfig.checkOutputOrder);
                if (resultTuples == testConfig.expectedTuples) {
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

void TestHelper::loadGraph(TestSuiteSystemConfig& config) {
    GraphLoader graphLoader(config.graphInputDir, config.graphOutputDir, config.maxNumThreads);
    graphLoader.loadGraph();
}

void BaseGraphLoadingTest::SetUp() {
    testSuiteSystemConfig.graphInputDir = getInputCSVDir();
    testSuiteSystemConfig.graphOutputDir = TEMP_TEST_DIR;
    TestHelper::loadGraph(testSuiteSystemConfig);
}

vector<string> TestHelper::getActualOutput(
    FactorizedTable& queryResult, vector<DataType> dataTypes, bool checkOutputOrder) {
    vector<string> actualOutput;
    if (queryResult.getTotalNumFlatTuples() != 0) {
        auto flatTupleIterator = queryResult.getFlatTupleIterator();
        while (flatTupleIterator.hasNextFlatTuple()) {
            FlatTuple tuple(dataTypes);
            flatTupleIterator.getNextFlatTuple(tuple);
            actualOutput.push_back(tuple.toString(vector<uint32_t>(tuple.len(), 0)));
        }
        if (!checkOutputOrder) {
            sort(actualOutput.begin(), actualOutput.end());
        }
    }
    return actualOutput;
}

} // namespace testing
} // namespace graphflow
