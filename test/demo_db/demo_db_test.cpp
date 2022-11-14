#include "test/test_utility/include/test_helper.h"

using ::testing::Test;
using namespace kuzu::testing;

class DemoDBTest : public DBTest {
public:
    string getInputCSVDir() override { return "dataset/demo-db/"; }
};

TEST_F(DemoDBTest, DemoDBTest) {
    runTest("test/test_files/demo_db/demo_db.test");
}

TEST_F(DemoDBTest, CreateRelTest) {
    conn->query("create (u:User {name: 'Alice'})");
    conn->query("MATCH (a:User) WHERE a.name = 'Adam' WITH a  MATCH (b:User) WHERE b.name = "
                "'Alice' WITH a, b CREATE (a)-[e:Follows {since:1990}]->(b)");
    auto result = conn->query(
        "MATCH (a:User)-[:Follows]->(b:User) WHERE a.name = 'Adam' RETURN b.name ORDER BY b.name");
    auto groundTruth = vector<string>{"Alice", "Karissa", "Zhang"};
    ASSERT_EQ(TestHelper::convertResultToString(*result, true /* check order */), groundTruth);
}

TEST_F(DemoDBTest, CreateAvgNullTest) {
    conn->query(
        "MATCH (a:User) WHERE a.name = 'Adam' CREATE (a)-[:Follows]->(b:User {name:'Alice'})");
    auto result =
        conn->query("MATCH (a:User) WITH a, avg(a.age) AS b, SUM(a.age) AS c, COUNT(a.age) AS d, "
                    "COUNT(*) AS e RETURN a, b, c,d, e ORDER BY c DESC");
    auto groundTruth = vector<string>{"Alice||||0|1", "Zhang|50|50.000000|50|1|1",
        "Karissa|40|40.000000|40|1|1", "Adam|30|30.000000|30|1|1", "Noura|25|25.000000|25|1|1"};
    ASSERT_EQ(TestHelper::convertResultToString(*result, true /* check order */), groundTruth);
}
