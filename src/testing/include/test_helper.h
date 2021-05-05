#pragma once
#include <cstdint>
#include <string>

#include "src/main/include/embedded_server.h"
using namespace std;

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
};
} // namespace testing
} // namespace graphflow
