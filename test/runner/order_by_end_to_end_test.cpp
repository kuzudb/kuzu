#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

class OrderByTests : public BaseGraphLoadingTest {

public:
    string getInputCSVDir() override { return "dataset/order-by-tests/"; }

    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        systemConfig->largePageBufferPoolSize = (1ull << 22);
        createConn();
    }
};

TEST_F(OrderByTests, OrderByLargeDatasetTest) {
    vector<TestQueryConfig> queryConfigs;
    queryConfigs = TestHelper::parseTestFile(
        "test/runner/queries/order_by/order_by_large_dataset.test", true /* checkOutputOrder */);
    for (auto& queryConfig : queryConfigs) {
        queryConfig.checkOutputOrder = true;
    }
    ASSERT_TRUE(TestHelper::runTest(queryConfigs, *conn));
}
