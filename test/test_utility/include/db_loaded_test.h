#include "gtest/gtest.h"
#include "test/test_utility/include/test_helper.h"

using ::testing::Test;

namespace graphflow {
namespace testing {

class DBLoadedTest : public Test {
public:
    const string TEMP_TEST_DIR = "test/unittest_temp/";
    const string DATA_SET_DIR = "dataset/tinysnb/";
    void SetUp() override {
        graphflow::testing::TestHelper::createDirOrError(TEMP_TEST_DIR);
        auto testSuiteSystemConfig = make_unique<TestSuiteSystemConfig>();
        testSuiteSystemConfig->graphInputDir = DATA_SET_DIR;
        testSuiteSystemConfig->graphOutputDir = TEMP_TEST_DIR;
        defaultSystem = TestHelper::getInitializedSystem(*testSuiteSystemConfig);
    }

    void TearDown() override { graphflow::testing::TestHelper::removeDirOrError(TEMP_TEST_DIR); }

public:
    unique_ptr<System> defaultSystem;
};

} // namespace testing
} // namespace graphflow
