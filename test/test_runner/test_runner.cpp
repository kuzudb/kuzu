#include "test_runner/test_runner.h"

#include <fstream>

#include "common/exception/test.h"
#include "common/md5.h"
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

void replaceEnv(std::string& queryToReplace, const std::string& env) {
    auto envValue = std::getenv(env.c_str()); // NOLINT(*-mt-unsafe);
    if (envValue != nullptr) {
        StringUtils::replaceAll(queryToReplace, "${" + env + "}", envValue);
    }
}

bool TestRunner::testStatement(
    TestStatement* statement, Connection& conn, std::string& databasePath) {
    std::unique_ptr<PreparedStatement> preparedStatement;
    StringUtils::replaceAll(statement->query, "${DATABASE_PATH}", databasePath);
    StringUtils::replaceAll(statement->query, "${KUZU_ROOT_DIRECTORY}", KUZU_ROOT_DIRECTORY);
    replaceEnv(statement->query, "UW_S3_ACCESS_KEY_ID");
    replaceEnv(statement->query, "UW_S3_SECRET_ACCESS_KEY");
    replaceEnv(statement->query, "AWS_S3_ACCESS_KEY_ID");
    replaceEnv(statement->query, "AWS_S3_SECRET_ACCESS_KEY");
    replaceEnv(statement->query, "RUN_ID");
    auto parsedStatements = std::vector<std::unique_ptr<parser::Statement>>();
    try {
        parsedStatements = conn.parseQuery(statement->query);
    } catch (std::exception& exception) {
        auto errorPreparedStatement = conn.preparedStatementWithError(exception.what());
        return checkLogicalPlan(errorPreparedStatement, statement, conn, 0);
    }
    if (parsedStatements.empty()) {
        auto errorPreparedStatement =
            conn.preparedStatementWithError("Connection Exception: Query is empty.");
        return checkLogicalPlan(errorPreparedStatement, statement, conn, 0);
    }
    if (parsedStatements.size() > 1) {
        throw TestException("Current test framework does not support multiple query statements!");
    }
    auto parsedStatement = std::move(parsedStatements[0]);
    if (statement->encodedJoin.empty()) {
        preparedStatement = conn.prepareNoLock(parsedStatement.get(), statement->enumerate);
    } else {
        preparedStatement = conn.prepareNoLock(parsedStatement.get(), true, statement->encodedJoin);
    }
    // Check for wrong statements
    if (!statement->expectedError && !statement->expectedErrorRegex &&
        !preparedStatement->isSuccess()) {
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
    } else if (statement->expectedErrorRegex) {
        std::string expectedError = StringUtils::rtrim(result->getErrorMessage());
        std::regex pattern("^.*[\\\\/]+([^\\\\/]+)$");
        std::smatch match1;
        bool is_match1 = std::regex_match(expectedError, match1, pattern);
        std::smatch match2;
        bool is_match2 = std::regex_match(statement->errorMessage, match2, pattern);
        if (is_match1 && is_match2) {
            if (match1[1] == match2[1]) {
                return true;
            }
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
    std::vector<std::string> resultTuples =
        TestRunner::convertResultToString(*result, statement->checkOutputOrder);
    if (statement->expectHash) {
        std::string resultHash =
            TestRunner::convertResultToMD5Hash(*result, statement->checkOutputOrder);
        if (resultTuples.size() == result->getNumTuples() &&
            resultHash == statement->expectedHashValue &&
            resultTuples.size() == statement->expectedNumTuples) {
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
            spdlog::info("HASH: {}\n", resultHash);
            return false;
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
        // NOTE: If you wish to change this sorting in a
        // way that alters its result, you may break existing
        // hashed test cases
    }
    return actualOutput;
}

std::string TestRunner::convertResultToMD5Hash(QueryResult& queryResult, bool checkOutputOrder) {
    queryResult.resetIterator();
    MD5 hasher;
    std::vector<std::string> stringRep = convertResultToString(queryResult, checkOutputOrder);
    for (std::string line : stringRep) {
        hasher.addToMD5(line.c_str());
        hasher.addToMD5("\n");
    }
    return std::string(hasher.finishMD5());
}

std::unique_ptr<planner::LogicalPlan> TestRunner::getLogicalPlan(
    const std::string& query, kuzu::main::Connection& conn) {
    return std::move(conn.prepare(query)->logicalPlans[0]);
}

} // namespace testing
} // namespace kuzu
