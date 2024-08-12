#include "graph_test/graph_test.h"

namespace kuzu {
namespace testing {

class NodeUpdateTest : public EmptyDBTest {
protected:
    void SetUp() override {
        EmptyDBTest::SetUp();
        createDBAndConn();
    }

    void TearDown() override { EmptyDBTest::TearDown(); }
};

TEST_F(NodeUpdateTest, UpdateSameRow) {
    ASSERT_TRUE(conn->query("CREATE NODE TABLE IF NOT EXISTS Product (item STRING, price INT64, "
                            "PRIMARY KEY (item))")
                    ->isSuccess());
    ASSERT_TRUE(conn->query("CREATE (n:Product {item: 'watch'}) SET n.price = 100")->isSuccess());
    ASSERT_TRUE(
        conn->query("MATCH (n:Product) WHERE n.item = 'watch' SET n.price = 200")->isSuccess());
    ASSERT_TRUE(
        conn->query("MATCH (n:Product) WHERE n.item = 'watch' SET n.price = 300")->isSuccess());
    const auto res = conn->query("MATCH (n:Product) RETURN n.price");
    ASSERT_TRUE(res->isSuccess() && res->hasNext());
    ASSERT_EQ(res->getNext()->getValue(0)->val.int64Val, 300);
}

} // namespace testing
} // namespace kuzu
