#pragma once

#include <optional>
#include <set>
#include <string>
#include <unordered_map>
#include <vector>

#include "common/constants.h"

namespace kuzu {
namespace testing {

enum class ConcurrentStatusFlag {
    NONE,
    BEGIN,
    END,
};

enum class ManualUseDatasetFlag {
    NONE,
    SCHEMA,
    INSERT,
};

enum class TestStatementType {
    INVALID,
    VALID,
    LOG,
};

enum class ResultType {
    OK,
    HASH,
    TUPLES,
    CSV_FILE,
    ERROR_MSG,
    ERROR_REGEX,
};

struct TestQueryResult {
    ResultType type;
    uint64_t numTuples = 0;
    // errorMsg, CSVFile, hashValue uses first element
    std::vector<std::string> expectedResult;
    TestQueryResult(ResultType type, uint64_t numTuples, std::vector<std::string> expectedResult)
        : type{type}, numTuples{numTuples}, expectedResult{expectedResult} {}
    TestQueryResult() : type{ResultType::OK}, numTuples{0}, expectedResult{} {}
};

struct TestStatement {
    TestStatementType type = TestStatementType::INVALID;
    std::string logMessage;

    // For queries
    std::string query;
    bool checkOutputOrder = false;
    bool checkColumnNames = false;
    bool checkPrecision = false;
    std::vector<TestQueryResult> result;
    ManualUseDatasetFlag manualUseDataset = ManualUseDatasetFlag::NONE;
    std::string dataset;

    // For multi copy tests
    uint32_t multiCopySplits = 0;
    std::string multiCopyTable;
    std::string multiCopySource;
    std::vector<uint64_t> seed;

    // for multiple conns
    std::string batchStatementsCSVFile;
    std::optional<std::string> connName;
    bool reloadDBFlag = false;
    bool importDBFlag = false;
    ConcurrentStatusFlag connectionsStatusFlag = ConcurrentStatusFlag::NONE;

    // for export and import db
    std::string importFilePath;
    bool removeFileFlag = false;
    std::string removeFilePath;

    // Used in REWRITE_TESTS mode.
    ResultType testResultType;
    std::string testCaseName;
    std::string originalQuery;
    std::string newOutput;
    bool isPartOfStatementBlock = false;

    // Execute by default
    bool skipFSMLeakCheckerFlag = false;

    bool isValid() const { return type != TestStatementType::INVALID; }
};

// Test group is a collection of test cases in a single file.
// Each test case can have multiple statements (TestStatement).
// In GoogleTest, the behaviour would be equivalent to:
// TEST(TestGroup.group, TestGroup.testCases[name])
struct TestGroup {
    std::string group;
    std::string dataset;
    std::unordered_map<std::string, std::vector<TestStatement>> testCases;
    std::unordered_map<std::string, std::vector<TestStatement>> testCasesStatementBlocks;
    std::optional<uint64_t> bufferPoolSize;
    // for multiple connections
    uint64_t checkpointWaitTimeout = common::DEFAULT_CHECKPOINT_WAIT_TIMEOUT_IN_MICROS;
    std::unordered_map<std::string, std::set<std::string>> testCasesConnNames;
    bool testFwdOnly;

    enum class DatasetType { CSV, PARQUET, NPY, CSV_TO_PARQUET, TURTLE, KUZU, JSON, CSV_TO_JSON };
    DatasetType datasetType;

    bool isValid() const { return !group.empty() && !dataset.empty(); }
    bool hasStatements() const { return !testCases.empty(); }
};

} // namespace testing
} // namespace kuzu
