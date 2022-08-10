#include "test/test_utility/include/test_helper.h"

#include <fstream>

#include "spdlog/spdlog.h"

#include "src/loader/include/graph_loader.h"

using namespace std;
using namespace graphflow::planner;
using namespace graphflow::loader;

namespace graphflow {
namespace testing {

vector<unique_ptr<TestQueryConfig>> TestHelper::parseTestFile(
    const string& path, bool checkOutputOrder) {
    vector<unique_ptr<TestQueryConfig>> result;
    if (access(path.c_str(), 0) != 0) {
        throw Exception("Test file not exists! [" + path + "].");
    }
    struct stat status {};
    stat(path.c_str(), &status);
    if (status.st_mode & S_IFDIR) {
        throw Exception("Test file is a directory. [" + path + "].");
    }
    ifstream ifs(path);
    string line;
    TestQueryConfig* currentConfig;
    while (getline(ifs, line)) {
        if (line.starts_with("-NAME")) {
            auto config = make_unique<TestQueryConfig>();
            currentConfig = config.get();
            result.push_back(move(config));
            currentConfig->name = line.substr(6, line.length());
        } else if (line.starts_with("-QUERY")) {
            currentConfig->query = line.substr(7, line.length());
        } else if (line.starts_with("-PARALLELISM")) {
            currentConfig->numThreads = stoi(line.substr(13, line.length()));
        } else if (line.starts_with("-ENCODED_JOIN")) {
            currentConfig->encodedJoin = line.substr(14, line.length());
        } else if (line.starts_with("-ENUMERATE")) {
            currentConfig->enumerate = true;
        } else if (line.starts_with("----")) {
            uint64_t numTuples = stoi(line.substr(5, line.length()));
            currentConfig->expectedNumTuples = numTuples;
            for (auto i = 0u; i < numTuples; i++) {
                getline(ifs, line);
                currentConfig->expectedTuples.push_back(line);
            }
            if (!checkOutputOrder) { // order is not important for result
                sort(currentConfig->expectedTuples.begin(), currentConfig->expectedTuples.end());
            }
        }
    }
    return result;
}

bool TestHelper::testQueries(vector<unique_ptr<TestQueryConfig>>& configs, Connection& conn) {
    auto numPassedQueries = 0u;
    vector<uint64_t> failedQueries;
    for (auto i = 0u; i < configs.size(); i++) {
        if (testQuery(configs[i].get(), conn)) {
            numPassedQueries++;
        } else {
            failedQueries.push_back(i);
        }
    }
    spdlog::info("SUMMARY:");
    if (failedQueries.empty()) {
        spdlog::info("ALL QUERIES PASSED.");
    } else {
        for (auto& idx : failedQueries) {
            spdlog::info("QUERY {} NOT PASSED.", configs[idx]->name);
        }
    }
    return numPassedQueries == configs.size();
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

void TestHelper::initializeConnection(TestQueryConfig* config, Connection& conn) {
    spdlog::info("TEST: {}", config->name);
    spdlog::info("QUERY: {}", config->query);
    conn.setMaxNumThreadForExec(config->numThreads);
}

bool TestHelper::testQuery(TestQueryConfig* config, Connection& conn) {
    initializeConnection(config, conn);
    vector<unique_ptr<LogicalPlan>> plans;
    if (config->enumerate) {
        plans = conn.enumeratePlans(config->query);
    } else {
        plans.push_back(conn.getBestPlan(config->query));
    }
    assert(!plans.empty());
    auto numPassedPlans = 0u;
    for (auto i = 0u; i < plans.size(); ++i) {
        auto planStr = plans[i]->toString();
        auto result = conn.executePlan(move(plans[i]));
        assert(result->isSuccess());
        vector<string> resultTuples = convertResultToString(*result, config->checkOutputOrder);
        if (resultTuples.size() == result->getNumTuples() &&
            resultTuples == config->expectedTuples) {
            spdlog::info(
                "PLAN{} PASSED in {}ms.", i, result->getQuerySummary()->getExecutionTime());
            numPassedPlans++;
        } else {
            spdlog::error("PLAN{} NOT PASSED.", i);
            spdlog::info("PLAN: \n{}", planStr);
            spdlog::info("RESULT: \n");
            for (auto& tuple : resultTuples) {
                spdlog::info(tuple);
            }
        }
    }
    spdlog::info("{}/{} plans passed.", numPassedPlans, plans.size());
    return numPassedPlans == plans.size();
}

void BaseGraphLoadingTest::loadGraph() {
    GraphLoader graphLoader(getInputCSVDir(), TestHelper::TEMP_TEST_DIR,
        systemConfig->maxNumThreads, systemConfig->defaultPageBufferPoolSize,
        systemConfig->largePageBufferPoolSize);
    graphLoader.loadGraph();
}

void BaseGraphLoadingTest::commitOrRollbackConnection(bool isCommit, bool testRecovery) {
    if (!testRecovery) {
        if (isCommit) {
            conn->commit();
        } else {
            conn->rollback();
        }
        conn->beginWriteTransaction();
    } else {
        if (isCommit) {
            conn->commitButSkipCheckpointingForTestingRecovery();
        } else {
            conn->rollbackButSkipCheckpointingForTestingRecovery();
        }
    }
}

} // namespace testing
} // namespace graphflow
