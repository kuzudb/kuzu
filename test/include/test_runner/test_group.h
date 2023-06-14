#pragma once

#include "main/kuzu.h"

namespace kuzu {
namespace testing {

struct TestStatement {
    std::string name;
    std::string query;
    uint64_t numThreads = 4;
    std::string encodedJoin;
    uint64_t expectedNumTuples = 0;
    bool expectedError = false;
    bool expectedOk = false;
    std::vector<std::string> expectedTuples;
    std::string errorMessage;
    bool enumerate = false;
    bool checkOutputOrder = false;
    bool isBeginWriteTransaction = false;
    std::string expectedTuplesCSVFile;
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

    enum class DatasetType { CSV, PARQUET, NPY, CSV_TO_PARQUET };
    DatasetType datasetType;

    bool isValid() const { return !group.empty() && !dataset.empty(); }
    bool hasStatements() const { return !testCases.empty(); }
};

} // namespace testing
} // namespace kuzu
