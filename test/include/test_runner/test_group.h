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

struct TestStatement {
    std::string logMessage;
    std::string query;
    uint64_t numThreads = 4;
    std::string encodedJoin;
    bool expectedError = false;
    std::string errorMessage;
    bool expectedOk = false;
    uint64_t expectedNumTuples = 0;
    std::vector<std::string> expectedTuples;
    bool enumerate = false;
    bool checkOutputOrder = false;
    std::string expectedTuplesCSVFile;
    // for multiple conns
    std::string batchStatmentsCSVFile;
    std::optional<std::string> connName;
    bool reloadDBFlag = false;
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
    uint64_t checkpointWaitTimeout =
        common::DEFAULT_CHECKPOINT_WAIT_TIMEOUT_FOR_TRANSACTIONS_TO_LEAVE_IN_MICROS;
    std::unordered_map<std::string, std::set<std::string>> testCasesConnNames;

    enum class DatasetType { CSV, PARQUET, NPY, CSV_TO_PARQUET, TURTLE };
    DatasetType datasetType;

    bool isValid() const { return !group.empty() && !dataset.empty(); }
    bool hasStatements() const { return !testCases.empty(); }
};

} // namespace testing
} // namespace kuzu
