#include "test_runner/test_runner.h"

#include <fstream>

#include "common/exception/test.h"
#include "common/string_utils.h"
#include "gtest/gtest.h"
#include "planner/operator/logical_plan.h"
#include "spdlog/spdlog.h"
#include "test_helper/test_helper.h"

using namespace kuzu::main;
using namespace kuzu::common;

namespace kuzu {
namespace testing {

void TestRunner::runTest(TestStatement* statement, Connection& conn, std::string& databasePath) {
    // for batch statements
    if (!statement->batchStatmentsCSVFile.empty()) {
        TestHelper::executeScript(statement->batchStatmentsCSVFile, conn);
        return;
    }
    // for normal statement
    spdlog::info("DEBUG LOG: {}", statement->logMessage);
    spdlog::info("QUERY: {}", statement->query);
    conn.setMaxNumThreadForExec(statement->numThreads);
    ASSERT_TRUE(testStatement(statement, conn, databasePath));
}

bool TestRunner::testStatement(
    TestStatement* statement, Connection& conn, std::string& databasePath) {
    std::unique_ptr<PreparedStatement> preparedStatement;
    StringUtils::replaceAll(statement->query, "${DATABASE_PATH}", databasePath);
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
        if (!statement->checkOutputOrder) {
            sort(statement->expectedTuples.begin(), statement->expectedTuples.end());
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
