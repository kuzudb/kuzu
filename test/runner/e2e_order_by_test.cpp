#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

class OrderByTests : public DBTest {

public:
    string getInputCSVDir() override { return "dataset/order-by-tests/"; }
};

TEST_F(OrderByTests, OrderByLargeDatasetTest) {
    auto queryConfigs = TestHelper::parseTestFile(
        "test/test_files/order_by/order_by.test", true /* checkOutputOrder */);
    for (auto& queryConfig : queryConfigs) {
        queryConfig->checkOutputOrder = true;
    }
    ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
}
