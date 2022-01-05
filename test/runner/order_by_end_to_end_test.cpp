#include <filesystem>

#include "gtest/gtest.h"
#include "test/test_utility/include/test_helper.h"

using ::testing::Test;
using namespace graphflow::testing;

class OrderByTests : public DBLoadedTest {
public:
    string getInputCSVDir() override { return "dataset/order-by-tests/"; }
};

TEST_F(OrderByTests, OrderByLargeDatasetTest) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs = TestHelper::parseTestFile(
        "test/runner/queries/order_by/order_by_large_dataset.test", true /* checkOutputOrder */);
    for (auto& queryConfig : queryConfigs) {
        queryConfig.checkOutputOrder = true;
    }
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *defaultSystem));
}
