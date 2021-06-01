#include <filesystem>

#include "gtest/gtest.h"

#include "src/testing/include/test_helper.h"

using ::testing::Test;
using namespace graphflow::testing;

class TinySnbProcessorTest : public Test {

public:
    void SetUp() override {
        createDefaultSystemConfig();
        if (!filesystem::exists(defaultSystemConfig->graphOutputDir) &&
            !filesystem::create_directory(defaultSystemConfig->graphOutputDir)) {
            throw invalid_argument(
                "Graph output directory cannot be created. Check if it exists and remove it.");
        }
        defaultSystem = TestHelper::getInitializedSystem(*defaultSystemConfig);
    }

    void TearDown() override {
        error_code removeErrorCode;
        if (!filesystem::remove_all(defaultSystemConfig->graphOutputDir, removeErrorCode)) {
            spdlog::error("Remove graph output directory error: {}", removeErrorCode.message());
        }
    }

    void createDefaultSystemConfig() {
        defaultSystemConfig = make_unique<TestSuiteSystemConfig>();
        defaultSystemConfig->graphInputDir = "dataset/tinysnb/";
        defaultSystemConfig->graphOutputDir = "test/unittest_temp/";
    }

public:
    unique_ptr<TestSuiteSystemConfig> defaultSystemConfig;
    unique_ptr<System> defaultSystem;
};

TEST_F(TinySnbProcessorTest, StructuralQueries) {
    unique_ptr<TestSuiteQueryConfig> queryConfig;
    queryConfig = TestHelper::parseTestFile("test/runner/queries/structural/nodes.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
    queryConfig = TestHelper::parseTestFile("test/runner/queries/structural/paths.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
    queryConfig = TestHelper::parseTestFile("test/runner/queries/structural/stars.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
}

TEST_F(TinySnbProcessorTest, FilteredQueries) {
    unique_ptr<TestSuiteQueryConfig> queryConfig;
    queryConfig = TestHelper::parseTestFile("test/runner/queries/filtered/id_comparison.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
    queryConfig = TestHelper::parseTestFile("test/runner/queries/filtered/nodes.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
    queryConfig = TestHelper::parseTestFile("test/runner/queries/filtered/paths.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
    queryConfig = TestHelper::parseTestFile("test/runner/queries/filtered/stars.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
    queryConfig = TestHelper::parseTestFile("test/runner/queries/filtered/str_operations.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
    queryConfig =
        TestHelper::parseTestFile("test/runner/queries/filtered/unstructured_properties.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
    queryConfig = TestHelper::parseTestFile("test/runner/queries/filtered/load_csv.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
}

TEST_F(TinySnbProcessorTest, FrontierQueries) {
    unique_ptr<TestSuiteQueryConfig> queryConfig;
    queryConfig = TestHelper::parseTestFile("test/runner/queries/structural/frontier.test");
    ASSERT_TRUE(TestHelper::runTest(*queryConfig, *defaultSystem));
}
