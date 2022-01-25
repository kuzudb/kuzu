#pragma once

#include <cstdint>
#include <string>

#include "gtest/gtest.h"

#include "src/main/include/system.h"

using namespace std;
using namespace graphflow::main;
using ::testing::Test;

namespace graphflow {
namespace testing {

struct TestSuiteSystemConfig {
    string graphInputDir;
    string graphOutputDir;
    uint64_t maxNumThreads = 4;
    bool isInMemory = false;
};

struct TestQueryConfig {
    uint64_t numThreads = 1;
    string name;
    string query;
    uint64_t expectedNumTuples;
    vector<string> expectedTuples;
    bool checkOutputOrder = false;
};

class TestHelper {

public:
    static bool runTest(const vector<TestQueryConfig>& testConfigs, const System& system);

    static vector<TestQueryConfig> parseTestFile(const string& path, bool checkOutputOrder = false);

    static void loadGraph(TestSuiteSystemConfig& config);

    static unique_ptr<System> getInitializedSystem(TestSuiteSystemConfig& config) {
        return make_unique<System>(config.graphOutputDir, config.isInMemory);
    }

    static vector<string> getActualOutput(
        FactorizedTable& queryResult, bool checkOutputOrder = false);
};

class BaseGraphLoadingTest : public Test {
public:
    void SetUp() override;

    void TearDown() override { FileUtils::removeDir(testSuiteSystemConfig.graphOutputDir); }

    virtual string getInputCSVDir() = 0;

public:
    const string TEMP_TEST_DIR = "test/unittest_temp/";
    TestSuiteSystemConfig testSuiteSystemConfig;
};

class DBLoadedTest : public BaseGraphLoadingTest {
public:
    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        defaultSystem = TestHelper::getInitializedSystem(testSuiteSystemConfig);
    }

public:
    unique_ptr<System> defaultSystem;
};

class InMemoryDBLoadedTest : public BaseGraphLoadingTest {
public:
    void SetUp() override {
        graphflow::testing::BaseGraphLoadingTest::SetUp();
        testSuiteSystemConfig.isInMemory = true;
        defaultSystem = TestHelper::getInitializedSystem(testSuiteSystemConfig);
        NumericMetric numBufferHits(true), numBufferMisses(true), numIO(true);
        metrics = make_unique<BufferManagerMetrics>(numBufferHits, numBufferMisses, numIO);
    }

public:
    unique_ptr<System> defaultSystem;
    unique_ptr<BufferManagerMetrics> metrics;
};

} // namespace testing
} // namespace graphflow
