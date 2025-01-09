#pragma once

#include <memory>
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
};

struct TestStatement {
    std::string logMessage;
    std::string query;
    uint64_t numThreads = 4;
    bool checkOutputOrder = false;
    bool checkColumnNames = false;
    bool checkPrecision = false;
    std::vector<TestQueryResult> result;
    ManualUseDatasetFlag manualUseDataset;
    std::string dataset;
    uint32_t multiCopySplits = 0;
    std::string multiCopyTable;
    std::string multiCopySource;
    std::vector<uint64_t> seed;
    // for multiple conns
    std::string batchStatmentsCSVFile;
    std::optional<std::string> connName;
    bool reloadDBFlag = false;
    bool importDBFlag = false;
    ConcurrentStatusFlag connectionsStatusFlag = ConcurrentStatusFlag::NONE;
    // for export and import db
    std::string importFilePath;
    bool removeFileFlag = false;
    std::string removeFilePath;
};

// Test group is a collection of test cases in a single file.
// Each test case can have multiple statements (TestStatement).
// In GoogleTest, the behaviour would be equivalent to:
// TEST(TestGroup.group, TestGroup.testCases[name])
struct TestGroup {
    std::string group;
    std::string dataset;
    std::unordered_map<std::string, std::vector<std::unique_ptr<TestStatement>>> testCases;
    std::unordered_map<std::string, std::vector<std::unique_ptr<TestStatement>>>
        testCasesStatementBlocks;
    uint64_t bufferPoolSize = common::BufferPoolConstants::DEFAULT_BUFFER_POOL_SIZE_FOR_TESTING;
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
