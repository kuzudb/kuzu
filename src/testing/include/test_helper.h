#pragma once

#include <cstdint>
#include <string>

#include "src/main/include/system.h"

using namespace std;
using namespace graphflow::main;

namespace graphflow {
namespace testing {

struct TestSuiteSystemConfig {
    string graphInputDir;
    string graphOutputDir;
    uint64_t maxNumThreads = 1;
    uint64_t bufferPoolSize = DEFAULT_BUFFER_POOL_SIZE;
};

struct TestSuiteQueryConfig {
    uint64_t numThreads = 1;
    bool compareResult = false;
    vector<string> name;
    vector<string> query;
    vector<uint64_t> expectedNumTuples;
    vector<vector<string>> expectedTuples;
};

class TestHelper {

public:
    static bool runTest(const TestSuiteQueryConfig& testConfig, const System& system);

    static unique_ptr<TestSuiteQueryConfig> parseTestFile(const string& path);

    static unique_ptr<System> getInitializedSystem(TestSuiteSystemConfig& config);
};

} // namespace testing
} // namespace graphflow
