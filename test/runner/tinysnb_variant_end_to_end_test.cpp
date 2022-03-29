#include <filesystem>

#include "gtest/gtest.h"
#include "test/test_utility/include/test_helper.h"

using ::testing::Test;
using namespace graphflow::testing;

class TinySnbVarintEndToEndTest : public DBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/tinysnb-variant/"; }
};

TEST_F(TinySnbVarintEndToEndTest, StructuralCyclicQueries) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/structural/cyclic.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *conn));
}

TEST_F(TinySnbVarintEndToEndTest, FilteredCyclicQueries) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs = TestHelper::parseTestFile("test/runner/queries/filtered/cyclic.test");
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *conn));
}
