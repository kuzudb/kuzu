#include "test/test_utility/include/test_helper.h"

using namespace graphflow::testing;

class OrderByTests : public BaseGraphTest {

public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        systemConfig->largePageBufferPoolSize = (1ull << 23);
        createDBAndConn();
    }

    void initGraph() override {
        conn->query(createNodeCmdPrefix +
                    "person (ID INT64, studentID INT64, balance INT64, PRIMARY KEY (ID))");
        conn->query("COPY person FROM \"dataset/order-by-tests/vPerson.csv\"");
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
