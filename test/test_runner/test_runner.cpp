#include "test_runner/test_runner.h"

#include <fstream>

#include "common/assert.h"
#include "common/exception/test.h"
#include "common/md5.h"
#include "common/string_utils.h"
#include "graph_test/base_graph_test.h"
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

void TestRunner::runBatchStatements(TestStatement* statement, Connection& conn,
    const std::string& databasePath) {
    std::string& cypherScript = statement->batchStatementsCSVFile;
    std::cout << "cypherScript: " << cypherScript << std::endl;
    if (!std::filesystem::exists(cypherScript)) {
        std::cout << "cypherScript: " << cypherScript << " doesn't exist. Skipping..." << std::endl;
        return;
    }
    std::ifstream file(cypherScript);
    if (!file.is_open()) {
        throw Exception(stringFormat("Error opening file: {}, errno: {}.", cypherScript, errno));
    }
    std::string line;
    while (getline(file, line)) {
        statement->query = line;
        testStatement(statement, conn, databasePath);
    }
}

void TestRunner::runTest(TestStatement* statement, Connection& conn,
    const std::string& databasePath) {
    // for batch statements
    if (!statement->batchStatementsCSVFile.empty()) {
        TestRunner::runBatchStatements(statement, conn, databasePath);
        return;
    }
    // for normal statement
    testStatement(statement, conn, databasePath);
}

void replaceEnv(std::string& queryToReplace, const std::string& env) {
    const auto envValue = std::getenv(env.c_str()); // NOLINT(*-mt-unsafe);
    if (envValue != nullptr) {
        StringUtils::replaceAll(queryToReplace, "${" + env + "}", envValue);
    }
}

static std::string getParentPath(const std::string& dbPath) {
    return std::filesystem::path(dbPath).parent_path().string();
}

void TestRunner::testStatement(TestStatement* statement, Connection& conn,
    const std::string& databasePath) {
    spdlog::info("DEBUG LOG: {}", statement->logMessage);
    spdlog::info("QUERY: {}", statement->query);
    if (statement->numThreads) {
        conn.setMaxNumThreadForExec(*statement->numThreads);
    }
    StringUtils::replaceAll(statement->query, "${DATABASE_PATH}", getParentPath(databasePath));
    StringUtils::replaceAll(statement->query, "${KUZU_ROOT_DIRECTORY}", KUZU_ROOT_DIRECTORY);
    replaceEnv(statement->query, "AZURE_PUBLIC_CONTAINER");
    replaceEnv(statement->query, "AZURE_ACCOUNT_NAME");
    replaceEnv(statement->query, "UW_S3_ACCESS_KEY_ID");
    replaceEnv(statement->query, "UW_S3_SECRET_ACCESS_KEY");
    replaceEnv(statement->query, "AWS_S3_ACCESS_KEY_ID");
    replaceEnv(statement->query, "AWS_S3_SECRET_ACCESS_KEY");
    replaceEnv(statement->query, "GCS_ACCESS_KEY_ID");
    replaceEnv(statement->query, "GCS_SECRET_ACCESS_KEY");
    replaceEnv(statement->query, "OLLAMA_URL");
    replaceEnv(statement->query, "RUN_ID");
    const auto actualResult = conn.query(statement->query);
    QueryResult* currentQueryResult = actualResult.get();
    idx_t resultIdx = 0u;
    do {
        if (TestHelper::REWRITE_TESTS) {
            generateOutput(currentQueryResult, statement, resultIdx);
        } else {
            checkLogicalPlan(conn, currentQueryResult, statement, resultIdx);
        }
        currentQueryResult = currentQueryResult->getNextQueryResult();
        resultIdx++;
    } while (currentQueryResult);
}

void TestRunner::checkLogicalPlan(Connection& conn, QueryResult* queryResult,
    TestStatement* statement, size_t resultIdx) {
    const TestQueryResult& testAnswer =
        statement->result[std::min(resultIdx, statement->result.size() - 1)];
    std::string actualError;
    switch (testAnswer.type) {
    case ResultType::OK: {
        ASSERT_TRUE(queryResult->isSuccess())
            << "EXPECT OK BUT GOT ERROR: " << queryResult->getErrorMessage();
    } break;
    case ResultType::ERROR_MSG: {
        const std::string expectedError = StringUtils::rtrim(testAnswer.expectedResult[0]);
        EXPECT_FALSE(queryResult->isSuccess());
        actualError = StringUtils::rtrim(queryResult->getErrorMessage());
        ASSERT_EQ(actualError, expectedError);
    } break;
    case ResultType::ERROR_REGEX: {
        actualError = StringUtils::rtrim(queryResult->getErrorMessage());
        ASSERT_TRUE(std::regex_match(actualError, std::regex(testAnswer.expectedResult[0])))
            << "Expected error to match regex: " << testAnswer.expectedResult[0]
            << " actual error: " << actualError;
    } break;
    default: {
        ASSERT_TRUE(queryResult->isSuccess())
            << "Unexpected error for query: " << queryResult->getErrorMessage();
        checkPlanResult(conn, queryResult, statement, resultIdx);
    }
    }
}

void TestRunner::checkPlanResult(Connection& conn, QueryResult* result, TestStatement* statement,
    size_t resultIdx) {
    spdlog::info("Query execution took {}ms.", result->getQuerySummary()->getExecutionTime());
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
            outputFailedPlan(conn, statement);
            spdlog::info("TUPLE COUNT NOT MATCHING:");
            spdlog::info("    EXPECTED {} TUPLES IN ANSWER FILE.", testAnswer.numTuples);
            spdlog::info("    FOUND {} TUPLES IN ANSWER FILE.", testAnswer.expectedResult.size());
            return;
        }
    }
    std::vector<std::string> resultTuples =
        convertResultToString(*result, statement->checkOutputOrder, statement->checkColumnNames);
    uint64_t actualNumTuples = result->getNumTuples();
    if (statement->checkColumnNames) {
        actualNumTuples++;
    }
    EXPECT_EQ(resultTuples.size(), actualNumTuples);
    if (testAnswer.type == ResultType::HASH) {
        std::string resultHash = convertResultToMD5Hash(*result, statement->checkOutputOrder,
            statement->checkColumnNames);

        if (resultHash != testAnswer.expectedResult[0]) {
            outputFailedPlan(conn, statement);
            spdlog::info("RESULT: \n");
            for (auto& tuple : resultTuples) {
                spdlog::info(tuple);
            }
            ASSERT_EQ(resultHash, testAnswer.expectedResult[0]);
        }
    } else if (statement->checkPrecision) {
        ASSERT_TRUE(statement->checkOutputOrder)
            << "CHECK_ORDER MUST BE ENABLED FOR CHECK_PRECISION";
        EXPECT_TRUE(resultTuples.size() == testAnswer.numTuples);
        ASSERT_TRUE(TestRunner::checkResultNumeric(*result, statement, resultIdx));
    } else {
        if (!statement->checkOutputOrder) {
            std::sort(testAnswer.expectedResult.begin(), testAnswer.expectedResult.end());
        }
        if (resultTuples == testAnswer.expectedResult) {
            spdlog::info("QUERY PASSED.");
        } else {
            outputFailedPlan(conn, statement);
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
}

// Appends `result` as the expected test output to `statement->newOuput` based on the given output
// type. Used in `EndToEndTest::rewriteTests` to rewrite outputs in test files.
void TestRunner::generateOutput(QueryResult* result, TestStatement* statement, size_t resultIdx) {
    TestQueryResult& testAnswer = statement->result[resultIdx];
    statement->testResultType = testAnswer.type;
    switch (testAnswer.type) {
    case ResultType::OK: {
        statement->newOutput +=
            "---- " + (result->isSuccess() ? std::string("ok") : std::string("error")) + '\n';
        if (!result->isSuccess()) {
            statement->newOutput += result->getErrorMessage() + '\n';
        }
    } break;
    case ResultType::HASH: {
        std::string resultHash = convertResultToMD5Hash(*result, statement->checkOutputOrder,
            statement->checkColumnNames);
        statement->newOutput += "---- hash\n" + std::to_string(result->getNumTuples()) +
                                " tuples hashed to " + resultHash + '\n';
    } break;
    case ResultType::TUPLES: {
        statement->newOutput +=
            "---- " + std::to_string(result->getNumTuples() + statement->checkColumnNames) + '\n';
        std::vector<std::string> resultTuples = convertResultToString(*result,
            statement->checkOutputOrder, statement->checkColumnNames);

        // Use the test file output if it otherwise matches the actual output. This allows
        // preserving the existing user-written order and avoids producing unnecessary diffs.
        auto orderedResults = testAnswer.expectedResult;
        if (!statement->checkOutputOrder) {
            std::sort(orderedResults.begin(), orderedResults.end());
        }
        auto& output = resultTuples == orderedResults ? testAnswer.expectedResult : resultTuples;

        for (auto& res : output) {
            statement->newOutput += res + '\n';
        }
    } break;
    case ResultType::CSV_FILE:
        // Not supported yet.
        return;
    case ResultType::ERROR_MSG: {
        statement->newOutput +=
            "---- " + (result->isSuccess() ? std::string("ok") : std::string("error")) + '\n';
        statement->newOutput += StringUtils::rtrim(result->getErrorMessage()) + '\n';
    } break;
    case ResultType::ERROR_REGEX: {
        statement->newOutput +=
            "---- " + (result->isSuccess() ? std::string("ok") : std::string("error(regex)")) +
            '\n';
    } break;
    }
}

void TestRunner::outputFailedPlan(Connection& conn, const TestStatement* statement) {
    spdlog::error("QUERY FAILED.");
    const auto plan = getLogicalPlan(statement->query, conn);
    if (plan) {
        spdlog::info("PLAN: \n{}", plan->toString());
    }
}

bool TestRunner::checkResultNumeric(QueryResult& queryResult, const TestStatement* statement,
    size_t resultIdx) {
    queryResult.resetIterator();
    const std::vector<LogicalType> dataTypes = queryResult.getColumnDataTypes();
    const TestQueryResult& testAnswer = statement->result[resultIdx];
    int rowIdx = statement->checkColumnNames;
    while (queryResult.hasNext()) {
        const auto actualTuple = queryResult.getNext();
        auto testTuple = StringUtils::split(testAnswer.expectedResult[rowIdx], "|");
        if (actualTuple->len() != testTuple.size()) {
            return false;
        }
        for (uint32_t i = 0; i < dataTypes.size(); i++) {
            const auto curValue = actualTuple->getValue(i);
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
        const auto tuple = queryResult.getNext();
        actualOutput.push_back(tuple->toString(std::vector<uint32_t>(tuple->len(), 0)));
    }
    if (!checkOutputOrder) {
        std::sort(actualOutput.begin(), actualOutput.end());
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
    const std::vector<std::string> stringRep =
        convertResultToString(queryResult, checkOutputOrder, checkColumnNames);
    const std::string lineBreaker = "\n";
    for (std::string line : stringRep) {
        hasher.addToMD5(line.c_str(), line.size());
        hasher.addToMD5(lineBreaker.c_str(), lineBreaker.size());
    }
    return std::string(hasher.finishMD5());
}

std::string TestRunner::convertResultColumnsToString(const QueryResult& queryResult) {
    std::string columnsString;
    const std::vector<std::string> columnNames = queryResult.getColumnNames();
    for (auto i = 0ul; i < columnNames.size(); i++) {
        if (i != 0) {
            columnsString += "|";
        }
        columnsString += columnNames[i];
    }
    return columnsString;
}

std::unique_ptr<planner::LogicalPlan> TestRunner::getLogicalPlan(const std::string& query,
    Connection& conn) {
    const auto preparedStatement = conn.prepare(query);
    KU_ASSERT(preparedStatement->isSuccess());
    return std::move(preparedStatement->logicalPlan);
}

} // namespace testing
} // namespace kuzu
