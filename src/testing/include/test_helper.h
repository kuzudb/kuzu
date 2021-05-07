#pragma once

#include <cstdint>
#include <string>

#include "src/main/include/system.h"

using namespace std;
using namespace graphflow::main;

namespace graphflow {
namespace testing {

struct TestSuiteConfig {
    string graphInputDir;
    string graphOutputDir;
    uint64_t numThreads = 1;
    uint64_t bufferPoolSize = DEFAULT_BUFFER_POOL_SIZE;
    bool compareResult = false;
    vector<string> name;
    vector<string> query;
    vector<uint64_t> expectedNumTuples;
    vector<vector<string>> expectedTuples;
    vector<string> expectedErrorMsgs;
};

class TestHelper {
public:
    static bool runTest(const string& path);

    static bool runExceptionTest(const string& path);

private:
    static TestSuiteConfig parseTestFile(const string& path);

    static unique_ptr<System> getInitializedSystem(TestSuiteConfig& testConfig);
};

} // namespace testing
} // namespace graphflow
