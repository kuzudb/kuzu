#include "test_helper/test_helper.h"

using namespace kuzu::testing;

class OrderByTests : public DBTest {

public:
    string getInputCSVDir() override {
        return TestHelper::appendKuzuRootPath("dataset/order-by-tests/");
    }
};

TEST_F(OrderByTests, OrderByLargeDatasetTest) {
    auto queryConfigs = TestHelper::parseTestFile(
        TestHelper::appendKuzuRootPath("test/test_files/order_by/order_by.test"),
        true /* checkOutputOrder */);
    for (auto& queryConfig : queryConfigs) {
        queryConfig->checkOutputOrder = true;
    }
    ASSERT_TRUE(TestHelper::testQueries(queryConfigs, *conn));
}
