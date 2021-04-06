#pragma once
#include <cstdint>
#include <string>

#include "src/runner/include/server/embedded_server.h"
using namespace std;

namespace graphflow {
namespace testing {

struct TestSuiteConfig {
    string graphInputDir;
    uint64_t numThreads = 1;
    uint64_t bufferPoolSize = DEFAULT_BUFFER_POOL_SIZE;
    vector<string> name;
    vector<string> query;
    vector<uint64_t> expectedNumTuples;
};

class TestHelper {
public:
    static bool runTest(const string& path);

private:
    static TestSuiteConfig parseTestFile(const string& path);
};
} // namespace testing
} // namespace graphflow
