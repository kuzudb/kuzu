#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

class OrderByTests : public BaseGraphLoadingTest {

public:
    string getInputCSVDir() override { return "dataset/order-by-tests/"; }

    void SetUp() override {
        BaseGraphLoadingTest::SetUp();
        systemConfig->largePageBufferPoolSize = (1ull << 23);
        createDBAndConn();
    }
};

TEST_F(OrderByTests, OrderByLargeDatasetTest) {
    auto queryConfigs = TestHelper::parseTestFile(
        "test/test_files/order_by/order_by.test", true /* checkOutputOrder */);
    for (auto& queryConfig : queryConfigs) {
        queryConfig->checkOutputOrder = true;
    }
    ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
}
