#include "test_helper/test_runner.h"

#include <iostream> // REMOVE ME

namespace kuzu {
namespace testing {

void TestRunner::runTest(const std::vector<std::unique_ptr<TestCommand>>& commands, Connection& conn) {

//    for (auto& command : commands) {
//        std::cout << "TEST: " << command->name << std::endl;
//    }
    // go over each TestCommand
    // read each property from the struct
    // ASSERT_TRUE based on what was specified
}

/*
bool TestRunner::testQueries(
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
            conn.prepareNoLock(config->query, true, config->encodedJoin); // enumerate
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
*/

} // namespace testing
} // namespace kuzu
