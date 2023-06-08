#include "test_runner/test_runner.h"

#include "common/string_utils.h"
#include "spdlog/spdlog.h"

using namespace kuzu::main;
using namespace kuzu::common;

namespace kuzu {
namespace testing {

void TestRunner::runTest(
    const std::vector<std::unique_ptr<TestStatement>>& statements, Connection& conn) {
    for (auto& statement : statements) {
        initializeConnection(statement.get(), conn);
        if (statement->isBeginWriteTransaction) {
            conn.beginWriteTransaction();
            continue;
        }
        ASSERT_TRUE(testStatement(statement.get(), conn));
    }
}

void TestRunner::initializeConnection(TestStatement* statement, Connection& conn) {
    spdlog::info("TEST: {}", statement->name);
    spdlog::info("QUERY: {}", statement->query);
    conn.setMaxNumThreadForExec(statement->numThreads);
}

bool TestRunner::testStatement(TestStatement* statement, Connection& conn) {
    std::unique_ptr<PreparedStatement> preparedStatement;
    if (statement->encodedJoin.empty()) {
        preparedStatement = conn.prepareNoLock(statement->query, statement->enumerate);
    } else {
        preparedStatement = conn.prepareNoLock(statement->query, true, statement->encodedJoin);
    }
    // Check for wrong statements
    if (!statement->expectedError && !preparedStatement->isSuccess()) {
        spdlog::error(preparedStatement->getErrorMessage());
        return false;
    }
    return checkLogicalPlans(preparedStatement, statement, conn);
}

bool TestRunner::checkLogicalPlans(std::unique_ptr<PreparedStatement>& preparedStatement,
    TestStatement* statement, Connection& conn) {
    auto numPlans = preparedStatement->logicalPlans.size();
    auto numPassedPlans = 0u;
    if (numPlans == 0) {
        return checkLogicalPlan(preparedStatement, statement, conn, 0);
    }
    for (auto i = 0u; i < numPlans; ++i) {
        if (checkLogicalPlan(preparedStatement, statement, conn, i)) {
            numPassedPlans++;
        }
    }
    return numPassedPlans == numPlans;
}

bool TestRunner::checkLogicalPlan(std::unique_ptr<PreparedStatement>& preparedStatement,
    TestStatement* statement, Connection& conn, uint32_t planIdx) {
    auto result = conn.executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get(), planIdx);
    if (statement->expectedError) {
        std::string expectedError = StringUtils::rtrim(result->getErrorMessage());
        if (statement->errorMessage == expectedError) {
            return true;
        }
        spdlog::info("EXPECTED ERROR: {}", expectedError);
    } else if (statement->expectedOk && result->isSuccess()) {
        return true;
    } else {
        auto planStr = preparedStatement->logicalPlans[planIdx]->toString();
        if (checkPlanResult(result, statement, planStr, planIdx)) {
            return true;
        }
    }
    return false;
}

bool TestRunner::checkPlanResult(std::unique_ptr<QueryResult>& result, TestStatement* statement,
    const std::string& planStr, uint32_t planIdx) {
    std::vector<std::string> resultTuples =
        TestRunner::convertResultToString(*result, statement->checkOutputOrder);

    if (!statement->expectedTuplesCSVFile.empty()) {
        std::ifstream expectedTuplesFile(statement->expectedTuplesCSVFile);
        if (!expectedTuplesFile.is_open()) {
            throw TestException("Cannot open file: " + statement->expectedTuplesCSVFile);
        }
        std::string line;
        while (std::getline(expectedTuplesFile, line)) {
            statement->expectedTuples.push_back(line);
        }
    }

    if (resultTuples.size() == result->getNumTuples() &&
        resultTuples == statement->expectedTuples) {
        spdlog::info(
            "PLAN{} PASSED in {}ms.", planIdx, result->getQuerySummary()->getExecutionTime());
        return true;
    } else {
        spdlog::error("PLAN{} NOT PASSED.", planIdx);
        spdlog::info("PLAN: \n{}", planStr);
        spdlog::info("RESULT: \n");
        for (auto& tuple : resultTuples) {
            spdlog::info(tuple);
        }
    }
    return false;
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
