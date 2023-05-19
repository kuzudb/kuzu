#pragma once

#include "main/kuzu.h"

using namespace kuzu::main;

namespace kuzu {
namespace testing {

struct TestStatement {
    std::string name;
    std::string query;
    std::string blockName;
    uint64_t numThreads = 4;
    std::string encodedJoin;
    uint64_t expectedNumTuples = 0;
    bool expectedError = false;
    bool expectedOk = false;
    std::vector<std::string> expectedTuples;
    std::string errorMessage;
    bool enumerate = false;
    bool checkOutputOrder = false;
    bool isVariableStatement = false;
};

struct TestCase {
    std::string group;
    std::string name;
    std::string dataset;
    std::vector<std::unique_ptr<TestStatement>> statements;
    std::vector<std::unique_ptr<TestStatement>> variableStatements;

    bool skipTest = false;

    bool isValid() const { return !group.empty() && !name.empty() && !dataset.empty(); }

    bool hasStatements() const { return !statements.empty(); }
};

} // namespace testing
} // namespace kuzu
