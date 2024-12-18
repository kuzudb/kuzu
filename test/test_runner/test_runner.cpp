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
    testStatement(statement, conn, databasePath);
}

void replaceEnv(std::string& queryToReplace, const std::string& env) {
    auto envValue = std::getenv(env.c_str()); // NOLINT(*-mt-unsafe);
    if (envValue != nullptr) {
        StringUtils::replaceAll(queryToReplace, "${" + env + "}", envValue);
    }
}

void TestRunner::testStatement(TestStatement* statement, Connection& conn,
    std::string& databasePath) {
    std::unique_ptr<PreparedStatement> preparedStatement;
    StringUtils::replaceAll(statement->query, "${DATABASE_PATH}", databasePath);
    StringUtils::replaceAll(statement->query, "${KUZU_ROOT_DIRECTORY}", KUZU_ROOT_DIRECTORY);
    replaceEnv(statement->query, "UW_S3_ACCESS_KEY_ID");
    replaceEnv(statement->query, "UW_S3_SECRET_ACCESS_KEY");
    replaceEnv(statement->query, "AWS_S3_ACCESS_KEY_ID");
    replaceEnv(statement->query, "AWS_S3_SECRET_ACCESS_KEY");
    replaceEnv(statement->query, "RUN_ID");
    auto parsedStatements = std::vector<std::shared_ptr<parser::Statement>>();
    try {
        parsedStatements = conn.getClientContext()->parseQuery(statement->query);
    } catch (std::exception& exception) {
        auto errorPreparedStatement = conn.preparedStatementWithError(exception.what());
        return checkLogicalPlan(errorPreparedStatement, statement, 0, conn, 0);
    }
    if (parsedStatements.empty()) {
        auto errorPreparedStatement =
            conn.preparedStatementWithError("Connection Exception: Query is empty.");
        return checkLogicalPlan(errorPreparedStatement, statement, 0, conn, 0);
    }

    size_t numParsed = parsedStatements.size();
    for (size_t i = 0; i < numParsed; i++) {
        auto parsedStatement = std::move(parsedStatements[i]);
        if (statement->encodedJoin.empty()) {
            preparedStatement = conn.prepareNoLock(parsedStatement, statement->enumerate);
        } else {
            preparedStatement = conn.prepareNoLock(parsedStatement, true, statement->encodedJoin);
        }
        // Check for wrong statements
        ResultType resultType = statement->result[std::min(i, statement->result.size() - 1)].type;
        if (resultType != ResultType::ERROR_MSG && resultType != ResultType::ERROR_REGEX) {
            ASSERT_TRUE(preparedStatement->isSuccess()) << preparedStatement->getErrorMessage();
        }
        checkLogicalPlans(preparedStatement, statement, i, conn);
    }
}

void TestRunner::checkLogicalPlans(std::unique_ptr<PreparedStatement>& preparedStatement,
    TestStatement* statement, size_t resultIdx, Connection& conn) {
    auto numPlans = preparedStatement->logicalPlans.size();
    if (numPlans == 0) {
        return checkLogicalPlan(preparedStatement, statement, resultIdx, conn, 0);
    }
    for (auto i = 0u; i < numPlans; ++i) {
        try {
            checkLogicalPlan(preparedStatement, statement, resultIdx, conn, i);
        } catch (std::exception& ex) {
            spdlog::error("PLAN {} FAILED.", i);
            throw;
        }
    }
}

void TestRunner::checkLogicalPlan(std::unique_ptr<PreparedStatement>& preparedStatement,
    TestStatement* statement, size_t resultIdx, Connection& conn, uint32_t planIdx) {
    auto result = conn.executeAndAutoCommitIfNecessaryNoLock(preparedStatement.get(), planIdx);
    // TODO(Ziyi): Our current testing framework is not able to handle multi-statements in a single
    // query.
    TestQueryResult& testAnswer =
        statement->result[std::min(resultIdx, statement->result.size() - 1)];
    std::string actualError;
    switch (testAnswer.type) {
    case ResultType::OK: {
        ASSERT_TRUE(result->isSuccess())
            << "EXPECT OK BUT GOT ERROR: " << result->getErrorMessage();
        break;
    }
    case ResultType::ERROR_MSG: {
        std::string expectedError = StringUtils::rtrim(testAnswer.expectedResult[0]);
        EXPECT_FALSE(result->isSuccess());
        actualError = StringUtils::rtrim(result->getErrorMessage());
        ASSERT_EQ(actualError, expectedError);
        break;
    }
    case ResultType::ERROR_REGEX: {
        actualError = StringUtils::rtrim(result->getErrorMessage());
        ASSERT_TRUE(std::regex_match(actualError, std::regex(testAnswer.expectedResult[0])))
            << "Expected error to match regex: " << testAnswer.expectedResult[0]
            << " actual error: " << actualError;
        break;
    }
    default: {
        ASSERT_TRUE(preparedStatement->success)
            << "Query compilation failed with error: " << preparedStatement->getErrorMessage();
        ASSERT_TRUE(result->isSuccess())
            << "Unexpected error for query: " << result->getErrorMessage();
        auto planStr = preparedStatement->logicalPlans[planIdx]->toString();
        checkPlanResult(result, statement, resultIdx, planStr, planIdx);
        break;
    }
    }
}

void TestRunner::checkPlanResult(std::unique_ptr<QueryResult>& result, TestStatement* statement,
    size_t resultIdx, const std::string& planStr, uint32_t planIdx) {
    spdlog::info("PLAN{} took {}ms.", planIdx, result->getQuerySummary()->getExecutionTime());
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
            spdlog::error("PLAN{} FAILED.", planIdx);
            spdlog::info("PLAN: \n{}", planStr);
            spdlog::info("TUPLE COUNT NOT MATCHING:");
            spdlog::info("    EXPECTED {} TUPLES IN ANSWER FILE.", testAnswer.numTuples);
            spdlog::info("    FOUND {} TUPLES IN ANSWER FILE.", testAnswer.expectedResult.size());
            return;
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
    EXPECT_EQ(resultTuples.size(), actualNumTuples);
    if (testAnswer.type == ResultType::HASH) {
        std::string resultHash = TestRunner::convertResultToMD5Hash(*result,
            statement->checkOutputOrder, statement->checkColumnNames);

        if (resultHash != testAnswer.expectedResult[0]) {
            spdlog::error("PLAN{} FAILED.", planIdx);
            spdlog::info("PLAN: \n{}", planStr);
            spdlog::info("RESULT: \n");
            for (auto& tuple : resultTuples) {
                spdlog::info(tuple);
            }
            ASSERT_EQ(resultHash, testAnswer.expectedResult[0]);
        }
    } else if (statement->checkPrecision) {
        ASSERT_TRUE(statement->checkOutputOrder)
            << "PLAN: \n"
            << planStr << "\nCHECK_ORDER MUST BE ENABLED FOR CHECK_PRECISION";
        EXPECT_TRUE(resultTuples.size() == testAnswer.numTuples);
        ASSERT_TRUE(TestRunner::checkResultNumeric(*result, statement, resultIdx));
    } else if (resultTuples == testAnswer.expectedResult) {
        spdlog::info("PLAN{} PASSED.", planIdx);
    } else {
        spdlog::error("PLAN{} FAILED.", planIdx);
        spdlog::info("PLAN: \n{}", planStr);
        if (resultTuples.size() == testAnswer.numTuples) {
            for (auto& tuple : resultTuples) {
                spdlog::info(tuple);
            }
            for (auto i = 0u; i < resultTuples.size(); i++) {
                EXPECT_EQ(resultTuples[i], testAnswer.expectedResult[i])
                    << "Result tuple at index " << i << " did not match the expected value";
            }
        } else {
            EXPECT_EQ(resultTuples.size(), actualNumTuples);
            ASSERT_EQ(resultTuples, testAnswer.expectedResult);
        }
    }
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
                    spdlog::error("Mismatched float value. Actual value: {}, Expected value: {}",
                        curValue->getValue<float>(), std::stof(testTuple[i]));
                    return false;
                }
                break;
            }
            case LogicalTypeID::DOUBLE: {
                if (!precisionEqual(curValue->getValue<double>(), std::stod(testTuple[i]))) {
                    spdlog::error("Mismatched double value. Actual value: {}, Expected value: {}",
                        curValue->getValue<double>(), std::stod(testTuple[i]));
                    return false;
                }
                break;
            }
            default: {
                if (curValue->toString() != testTuple[i]) {
                    spdlog::error("Mismatched value. Actual value: {}, Expected value: {}",
                        curValue->toString(), testTuple[i]);
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
