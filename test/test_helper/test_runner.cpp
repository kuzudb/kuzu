#include "test_helper/test_runner.h"

#include <iostream>

#include "spdlog/spdlog.h"
#include "test_helper/test_helper.h"

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

        std::cout << "A LA TUNGA: " << preparedStatement->getErrorMessage() << std::endl;
        std::cout << "ERROR: " << statement->errorMessage << std::endl;

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
            TestHelper::convertResultToString(*result, statement->checkOutputOrder);
        // statement->expectedTuplesFile is not empty: read expectedTuples from file
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

std::unique_ptr<planner::LogicalPlan> TestRunner::getLogicalPlan(
    const std::string& query, kuzu::main::Connection& conn) {
    return std::move(conn.prepare(query)->logicalPlans[0]);
}

} // namespace testing
} // namespace kuzu
