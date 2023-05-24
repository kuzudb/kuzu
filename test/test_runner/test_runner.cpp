#include "test_runner/test_runner.h"

#include "spdlog/spdlog.h"

using namespace kuzu::main;

namespace kuzu {
namespace testing {

void TestRunner::runTest(
    const std::vector<std::unique_ptr<TestStatement>>& statements, Connection& conn) {
    for (auto& statement : statements) {
        ASSERT_TRUE(testStatement(statement.get(), conn));
    }
}

void TestRunner::initializeConnection(TestStatement* statement, Connection& conn) {
    spdlog::info("TEST: {}", statement->name);
    spdlog::info("QUERY: {}", statement->query);
    conn.setMaxNumThreadForExec(statement->numThreads);
}

bool TestRunner::testStatement(TestStatement* statement, Connection& conn) {
    initializeConnection(statement, conn);
    std::unique_ptr<PreparedStatement> preparedStatement;
    if (statement->encodedJoin.empty()) {
        preparedStatement = conn.prepareNoLock(statement->query, statement->enumerate);
    } else {
        preparedStatement = conn.prepareNoLock(statement->query, true, statement->encodedJoin);
    }
    if (statement->expectedOk) {
        return preparedStatement->isSuccess();
    }
    if (statement->expectedError) {
        return (statement->errorMessage == preparedStatement->getErrorMessage());
    }
    if (!preparedStatement->isSuccess()) {
        spdlog::error(preparedStatement->getErrorMessage());
        return false;
    }
    return checkLogicalPlans(preparedStatement, statement, conn);
}

bool TestRunner::checkLogicalPlans(std::unique_ptr<PreparedStatement>& preparedStatement,
    TestStatement* statement, Connection& conn) {
    auto numPlans = preparedStatement->logicalPlans.size();
    auto numPassedPlans = 0u;
    for (auto i = 0u; i < numPlans; ++i) {
        auto planStr = preparedStatement->logicalPlans[i]->toString();
        auto result = conn.executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get(), i);
        assert(result->isSuccess());
        std::vector<std::string> resultTuples =
            TestRunner::convertResultToString(*result, statement->checkOutputOrder);
        if (resultTuples.size() == result->getNumTuples() &&
            resultTuples == statement->expectedTuples) {
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
    return numPassedPlans == numPlans;
}

std::vector<std::string> TestRunner::convertResultToString(
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

std::unique_ptr<planner::LogicalPlan> TestRunner::getLogicalPlan(
    const std::string& query, kuzu::main::Connection& conn) {
    return std::move(conn.prepare(query)->logicalPlans[0]);
}

} // namespace testing
} // namespace kuzu
