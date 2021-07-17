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
    uint64_t maxNumThreads = 1;
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

    static void loadGraph(TestSuiteSystemConfig& config);

    static unique_ptr<System> getInitializedSystem(TestSuiteSystemConfig& config);

    static void createDirOrError(const string& dir);

    static void removeDirOrError(const string& dir);
};

class BaseGraphLoadingTest : public Test {
public:
    void SetUp() override;

    void TearDown() override { TestHelper::removeDirOrError(TEMP_TEST_DIR); }

    virtual string getInputCSVDir() = 0;

public:
    const string TEMP_TEST_DIR = "test/unittest_temp/";

    TestSuiteSystemConfig testSuiteSystemConfig;
};

class BaseGraphLoadingTestWithCatalog : public BaseGraphLoadingTest {

public:
    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        catalog = make_unique<Catalog>(TEMP_TEST_DIR);
    }

public:
    unique_ptr<Catalog> catalog;
};

class DBLoadedTest : public BaseGraphLoadingTest {
public:
    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        defaultSystem = TestHelper::getInitializedSystem(testSuiteSystemConfig);
    }

    string getInputCSVDir() override { return "dataset/tinysnb/"; }

public:
    unique_ptr<System> defaultSystem;
};

} // namespace testing
} // namespace graphflow
