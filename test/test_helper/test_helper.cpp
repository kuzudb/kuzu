#include "test_helper/test_helper.h"

#include <fstream>

#include "json.hpp"
#include "spdlog/spdlog.h"

using namespace kuzu::common;
using namespace kuzu::planner;
using namespace kuzu::main;

namespace kuzu {
namespace testing {

std::vector<std::unique_ptr<TestQueryConfig>> TestHelper::parseTestFile(
    const std::string& path, bool checkOutputOrder) {
    std::vector<std::unique_ptr<TestQueryConfig>> result;
    if (access(path.c_str(), 0) != 0) {
        throw Exception("Test file not exists! [" + path + "].");
    }
    struct stat status {};
    stat(path.c_str(), &status);
    if (status.st_mode & S_IFDIR) {
        throw Exception("Test file is a directory. [" + path + "].");
    }
    std::ifstream ifs(path);
    std::string line;
    TestQueryConfig* currentConfig;
    while (getline(ifs, line)) {
        if (line.starts_with("-NAME")) {
            auto config = std::make_unique<TestQueryConfig>();
            currentConfig = config.get();
            result.push_back(std::move(config));
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

bool TestHelper::testQueries(
    std::vector<std::unique_ptr<TestQueryConfig>>& configs, Connection& conn) {
    spdlog::set_level(spdlog::level::info);
    auto numPassedQueries = 0u;
    std::vector<uint64_t> failedQueries;
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

std::vector<std::string> TestHelper::convertResultToString(
    QueryResult& queryResult, bool checkOutputOrder) {
    std::vector<std::string> actualOutput;
    while (queryResult.hasNext()) {
        auto tuple = queryResult.getNext();
        actualOutput.push_back(tuple->toString(std::vector<uint32_t>(tuple->len(), 0)));
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
    std::unique_ptr<PreparedStatement> preparedStatement;
    if (config->encodedJoin.empty()) {
        preparedStatement = conn.prepareNoLock(config->query, config->enumerate);
    } else {
        preparedStatement =
            conn.prepareNoLock(config->query, true /* enumerate*/, config->encodedJoin);
    }
    if (!preparedStatement->isSuccess()) {
        spdlog::error(preparedStatement->getErrorMessage());
        return false;
    }
    auto numPlans = preparedStatement->logicalPlans.size();
    if (numPlans == 0) {
        spdlog::error("Query {} has no plans" + config->name);
        return false;
    }
    auto numPassedPlans = 0u;
    for (auto i = 0u; i < numPlans; ++i) {
        auto planStr = preparedStatement->logicalPlans[i]->toString();
        auto result = conn.executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get(), i);
        assert(result->isSuccess());
        std::vector<std::string> resultTuples =
            convertResultToString(*result, config->checkOutputOrder);
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
    spdlog::info("{}/{} plans passed.", numPassedPlans, numPlans);
    return numPassedPlans == numPlans;
}

std::unique_ptr<planner::LogicalPlan> TestHelper::getLogicalPlan(
    const std::string& query, kuzu::main::Connection& conn) {
    return std::move(conn.prepare(query)->logicalPlans[0]);
}

} // namespace testing
} // namespace kuzu
