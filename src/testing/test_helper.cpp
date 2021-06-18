#include "src/testing/include/test_helper.h"

#include "spdlog/spdlog.h"

#include "src/loader/include/graph_loader.h"

using namespace std;
using namespace graphflow::planner;
using namespace graphflow::loader;

namespace graphflow {
namespace testing {

bool TestHelper::runTest(const TestSuiteQueryConfig& testConfig, const System& system) {
    auto sessionContext = SessionContext();
    auto numQueries = testConfig.query.size();
    uint64_t numPassedQueries = 0;
    vector<uint64_t> numPlansOfEachQuery(numQueries);
    vector<uint64_t> numPassedPlansOfEachQuery(numQueries);
    for (uint64_t i = 0; i < numQueries; i++) {
        spdlog::info("TEST: {}", testConfig.name[i]);
        spdlog::info("QUERY: {}", testConfig.query[i]);
        sessionContext.query = testConfig.query[i];
        auto plans = system.enumerateAllPlans(sessionContext);
        auto numPlans = plans.size();
        assert(numPlans > 0);
        numPlansOfEachQuery[i] = numPlans;
        uint64_t numPassedPlans = 0;
        for (uint64_t j = 0; j < numPlans; j++) {
            auto planStr = plans[j]->lastOperator->toString();
            auto result = system.executePlan(move(plans[j]), sessionContext);
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
                        for (uint64_t i = 0; i < numTuples; i++) {
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

unique_ptr<System> TestHelper::getInitializedSystem(TestSuiteSystemConfig& config) {
    {
        GraphLoader graphLoader(config.graphInputDir, config.graphOutputDir, config.maxNumThreads);
        graphLoader.loadGraph();
    }
    return make_unique<System>(config.graphOutputDir);
}

} // namespace testing
} // namespace graphflow
