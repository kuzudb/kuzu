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

template<typename T>
static bool precisionEqual(T x, T y) {
    // epsilon() gives gap size (ULP, unit in the last place) in interval [1, 2)
    // scale it to the gap size in interval [2^e, 2^{e+1}). e is min exponent
    const T m = std::min(std::fabs(x), std::fabs(y));

    // Subnormal numbers have fixed exponent, which is `min_exponent - 1`.
    const int exp = m < std::numeric_limits<T>::min() ? std::numeric_limits<T>::min_exponent - 1 :
                                                        std::ilogb(m);

    // Equal if abs difference is within 1 ULP
    return std::fabs(x - y) <= std::ldexp(std::numeric_limits<T>::epsilon(), exp);
}

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

bool TestRunner::testStatement(TestStatement* statement, Connection& conn,
    std::string& databasePath) {
    std::unique_ptr<PreparedStatement> preparedStatement;
    StringUtils::replaceAll(statement->query, "${DATABASE_PATH}", databasePath);
    StringUtils::replaceAll(statement->query, "${KUZU_ROOT_DIRECTORY}", KUZU_ROOT_DIRECTORY);
    replaceEnv(statement->query, "UW_S3_ACCESS_KEY_ID");
    replaceEnv(statement->query, "UW_S3_SECRET_ACCESS_KEY");
    replaceEnv(statement->query, "AWS_S3_ACCESS_KEY_ID");
    replaceEnv(statement->query, "AWS_S3_SECRET_ACCESS_KEY");
    replaceEnv(statement->query, "RUN_ID");
    std::unique_ptr<PreparedStatement> parsedStatement;
    if (statement->encodedJoin.empty()) {
        preparedStatement = conn.prepareInternal(statement->query, statement->enumerate, "");
    } else {
        preparedStatement = conn.prepareInternal(statement->query, true, statement->encodedJoin);
    }
    // Check for wrong statements
    auto numResults = statement->result.size();
    for (auto i = 0u; i < numResults; i++) {
        ResultType resultType = statement->result[i].type;
        if (resultType != ResultType::ERROR_MSG && resultType != ResultType::ERROR_REGEX &&
            !preparedStatement->isSuccess()) {
            spdlog::error(preparedStatement->getErrorMessage());
            return false;
        }
        if (!checkLogicalPlans(preparedStatement, statement, i, conn)) {
            return false;
        }
    }
    return true;
}

bool TestRunner::checkLogicalPlans(std::unique_ptr<PreparedStatement>& preparedStatement,
    TestStatement* statement, size_t resultIdx, Connection& conn) {
    auto numPlans = preparedStatement->logicalPlans.size();
    auto numPassedPlans = 0u;
    if (numPlans == 0) {
        return checkLogicalPlan(preparedStatement, statement, resultIdx, conn, 0);
    }
    for (auto i = 0u; i < numPlans; ++i) {
        if (checkLogicalPlan(preparedStatement, statement, resultIdx, conn, i)) {
            numPassedPlans++;
        }
    }
    return numPassedPlans == numPlans;
}

bool TestRunner::checkLogicalPlan(std::unique_ptr<PreparedStatement>& preparedStatement,
    TestStatement* statement, size_t resultIdx, Connection& conn, uint32_t planIdx) {
    auto result = conn.executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get(), planIdx);
    TestQueryResult& testAnswer = statement->result[resultIdx];
    std::string actualError;
    switch (testAnswer.type) {
    case ResultType::OK: {
        if (!result->isSuccess()) {
            spdlog::info("EXPECT OK BUT GOT ERROR: {}", result->getErrorMessage());
        }
        return result->isSuccess();
    }
    case ResultType::ERROR_MSG: {
        actualError = StringUtils::rtrim(result->getErrorMessage());
        if (testAnswer.expectedResult[0] == actualError) {
            return true;
        }
        spdlog::info("INCORRECT ERROR: {}", actualError);
        break;
    }
    case ResultType::ERROR_REGEX: {
        actualError = StringUtils::rtrim(result->getErrorMessage());
        if (std::regex_match(actualError, std::regex(testAnswer.expectedResult[0]))) {
            return true;
        }
        spdlog::info("INCORRECT ERROR: {}", actualError);
        break;
    }
    default: {
        if (!preparedStatement->success) {
            spdlog::info("Query compilation failed with error: {}",
                preparedStatement->getErrorMessage());
            return false;
        }
        auto planStr = preparedStatement->logicalPlans[planIdx]->toString();
        if (checkPlanResult(result, statement, resultIdx, planStr, planIdx)) {
            return true;
        }
        break;
    }
    }
    return false;
}

bool TestRunner::checkPlanResult(std::unique_ptr<QueryResult>& result, TestStatement* statement,
    size_t resultIdx, const std::string& planStr, uint32_t planIdx) {
    TestQueryResult& testAnswer = statement->result[resultIdx];
    if (testAnswer.type == ResultType::CSV_FILE) {
        std::ifstream expectedTuplesFile(testAnswer.expectedResult[0]);
        if (!expectedTuplesFile.is_open()) {
            throw TestException("Cannot open file: " + testAnswer.expectedResult[0]);
        }
        std::string line;
        testAnswer.expectedResult.clear();
        while (std::getline(expectedTuplesFile, line)) {
            testAnswer.expectedResult.push_back(line);
        }
        if (testAnswer.expectedResult.size() != testAnswer.numTuples) {
            spdlog::error("PLAN{} NOT PASSED.", planIdx);
            spdlog::info("PLAN: \n{}", planStr);
            spdlog::info("TUPLE COUNT NOT MATCHING:");
            spdlog::info("    EXPECTED {} TUPLES IN ANSWER FILE.", testAnswer.numTuples);
            spdlog::info("    FOUND {} TUPLES IN ANSWER FILE.", testAnswer.expectedResult.size());
            return false;
        }
        if (!statement->checkOutputOrder) {
            sort(testAnswer.expectedResult.begin(), testAnswer.expectedResult.end());
        }
    }
    std::vector<std::string> resultTuples = TestRunner::convertResultToString(*result,
        statement->checkOutputOrder, statement->checkColumnNames);
    uint64_t actualNumTuples = result->getNumTuples();
    if (statement->checkColumnNames) {
        actualNumTuples++;
    }
    if (testAnswer.type == ResultType::HASH) {
        std::string resultHash = TestRunner::convertResultToMD5Hash(*result,
            statement->checkOutputOrder, statement->checkColumnNames);
        if (resultTuples.size() == actualNumTuples && resultHash == testAnswer.expectedResult[0] &&
            resultTuples.size() == testAnswer.numTuples) {
            spdlog::info("PLAN{} PASSED in {}ms.", planIdx,
                result->getQuerySummary()->getExecutionTime());
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
    if (statement->checkPrecision) {
        if (!statement->checkOutputOrder) {
            spdlog::error("PLAN{} NOT PASSED.", planIdx);
            spdlog::info("PLAN: \n{}", planStr);
            spdlog::info("CHECK_ORDER MUST BE ENABLED FOR CHECK_PRECISION");
            return false;
        }
        if (resultTuples.size() == actualNumTuples && resultTuples.size() == testAnswer.numTuples &&
            TestRunner::checkResultNumeric(*result, statement, resultIdx)) {
            spdlog::info("PLAN{} PASSED in {}ms.", planIdx,
                result->getQuerySummary()->getExecutionTime());
            return true;
        }
    } else {
        if (resultTuples.size() == actualNumTuples && resultTuples == testAnswer.expectedResult) {
            spdlog::info("PLAN{} PASSED in {}ms.", planIdx,
                result->getQuerySummary()->getExecutionTime());
            return true;
        }
    }
    spdlog::error("PLAN{} NOT PASSED.", planIdx);
    spdlog::info("PLAN: \n{}", planStr);
    spdlog::info("RESULT: \n");
    for (auto& tuple : resultTuples) {
        spdlog::info(tuple);
    }
    return false;
}

bool TestRunner::checkResultNumeric(QueryResult& queryResult, TestStatement* statement,
    size_t resultIdx) {
    queryResult.resetIterator();
    std::vector<LogicalType> dataTypes = queryResult.getColumnDataTypes();
    TestQueryResult& testAnswer = statement->result[resultIdx];
    int rowIdx = statement->checkColumnNames;
    while (queryResult.hasNext()) {
        auto actualTuple = queryResult.getNext();
        auto testTuple = StringUtils::split(testAnswer.expectedResult[rowIdx], "|");
        if (actualTuple->len() != testTuple.size()) {
            return false;
        }
        for (uint32_t i = 0; i < dataTypes.size(); i++) {
            auto curValue = actualTuple->getValue(i);
            switch (dataTypes[i].getLogicalTypeID()) {
            case LogicalTypeID::FLOAT: {
                if (!precisionEqual(curValue->getValue<float>(), std::stof(testTuple[i]))) {
                    return false;
                }
                break;
            }
            case LogicalTypeID::DOUBLE: {
                if (!precisionEqual(curValue->getValue<double>(), std::stod(testTuple[i]))) {
                    return false;
                }
                break;
            }
            default: {
                if (curValue->toString() != testTuple[i]) {
                    return false;
                }
                break;
            }
            }
        }
        rowIdx++;
    }
    return true;
}

std::vector<std::string> TestRunner::convertResultToString(QueryResult& queryResult,
    bool checkOutputOrder, bool checkColumnNames) {
    std::vector<std::string> actualOutput;
    if (checkColumnNames) {
        actualOutput.push_back(convertResultColumnsToString(queryResult));
    }
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

std::string TestRunner::convertResultToMD5Hash(QueryResult& queryResult, bool checkOutputOrder,
    bool checkColumnNames) {
    queryResult.resetIterator();
    MD5 hasher;
    std::vector<std::string> stringRep =
        convertResultToString(queryResult, checkOutputOrder, checkColumnNames);
    std::string lineBreaker = "\n";
    for (std::string line : stringRep) {
        hasher.addToMD5(line.c_str(), line.size());
        hasher.addToMD5(lineBreaker.c_str(), lineBreaker.size());
    }
    return std::string(hasher.finishMD5());
}

std::string TestRunner::convertResultColumnsToString(main::QueryResult& queryResult) {
    std::string columnsString;
    std::vector<std::string> columnNames = queryResult.getColumnNames();
    for (auto i = 0ul; i < columnNames.size(); i++) {
        if (i != 0) {
            columnsString += "|";
        }
        columnsString += columnNames[i];
    }
    return columnsString;
}

std::unique_ptr<planner::LogicalPlan> TestRunner::getLogicalPlan(const std::string& query,
    kuzu::main::Connection& conn) {
    auto preparedStatement = conn.prepare(query);
    KU_ASSERT(preparedStatement->isSuccess());
    return std::move(preparedStatement->logicalPlans[0]);
}

} // namespace testing
} // namespace kuzu
