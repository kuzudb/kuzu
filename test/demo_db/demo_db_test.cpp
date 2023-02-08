#include "graph_test/graph_test.h"

using ::testing::Test;
using namespace kuzu::testing;

class DemoDBTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/demo-db/csv/");
    }
};

class DemoDBTestFromParquet : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/demo-db/parquet/");
    }
};

TEST_F(DemoDBTest, DemoDBTest) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/demo_db/demo_db.test"));
}

TEST_F(DemoDBTestFromParquet, DemoDBTest) {
    runTest(TestHelper::appendKuzuRootPath("test/test_files/demo_db/demo_db.test"));
}

TEST_F(DemoDBTest, CreateRelTest) {
    conn->query("create (u:User {name: 'Alice'})");
    conn->query("MATCH (a:User) WHERE a.name = 'Adam' WITH a  MATCH (b:User) WHERE b.name = "
                "'Alice' WITH a, b CREATE (a)-[e:Follows {since:1990}]->(b)");
    auto result = conn->query(
        "MATCH (a:User)-[:Follows]->(b:User) WHERE a.name = 'Adam' RETURN b.name ORDER BY b.name");
    auto groundTruth = std::vector<std::string>{"Alice", "Karissa", "Zhang"};
    ASSERT_EQ(TestHelper::convertResultToString(*result, true /* check order */), groundTruth);
}

TEST_F(DemoDBTest, CreateAvgNullTest) {
    conn->query(
        "MATCH (a:User) WHERE a.name = 'Adam' CREATE (a)-[:Follows]->(b:User {name:'Alice'})");
    auto result =
        conn->query("MATCH (a:User) WITH a, avg(a.age) AS b, SUM(a.age) AS c, COUNT(a.age) AS d, "
                    "COUNT(*) AS e RETURN a, b, c,d, e ORDER BY c DESC");
    auto groundTruth = std::vector<std::string>{"(label:User, 0:4, {name:Alice, age:})|||0|1",
        "(label:User, 0:2, {name:Zhang, age:50})|50.000000|50|1|1",
        "(label:User, 0:1, {name:Karissa, age:40})|40.000000|40|1|1",
        "(label:User, 0:0, {name:Adam, age:30})|30.000000|30|1|1",
        "(label:User, 0:3, {name:Noura, age:25})|25.000000|25|1|1"};
    ASSERT_EQ(TestHelper::convertResultToString(*result, true /* check order */), groundTruth);
}

TEST_F(DemoDBTest, DeleteNodeTest) {
    ASSERT_TRUE(conn->query("CREATE (u:User {name: 'Alice', age: 35});")->isSuccess());
    auto result = conn->query("MATCH (u:User) RETURN COUNT(*)");
    auto groundTruth = std::vector<std::string>{"5"};
    ASSERT_EQ(TestHelper::convertResultToString(*result, true /* check order */), groundTruth);
    ASSERT_TRUE(conn->query("MATCH (u:User) WHERE u.name = 'Alice' DELETE u;")->isSuccess());
    result = conn->query("MATCH (u:User) RETURN COUNT(*)");
    groundTruth = std::vector<std::string>{"4"};
    ASSERT_EQ(TestHelper::convertResultToString(*result, true /* check order */), groundTruth);
}

TEST_F(DemoDBTest, DeleteWithExceptionTest) {
    auto result = conn->query("MATCH (u:User) WHERE u.name = 'Adam' DELETE u;");
    ASSERT_FALSE(result->isSuccess());
    ASSERT_EQ(result->getErrorMessage(),
        "Runtime exception: Currently deleting a node with edges is not supported. node table 0 "
        "nodeOffset 0 has 1 (one-to-many or many-to-many) edges for edge file: " +
            TestHelper::appendKuzuRootPath("test/unittest_temp/r-3-0.lists."));
}

TEST_F(DemoDBTest, SetNodeTest) {
    ASSERT_TRUE(conn->query("MATCH (u:User) WHERE u.name = 'Adam' SET u.age = 50")->isSuccess());
    auto result = conn->query("MATCH (u:User) WHERE u.name='Adam' RETURN u.age");
    auto groundTruth = std::vector<std::string>{"50"};
    ASSERT_EQ(TestHelper::convertResultToString(*result, true /* check order */), groundTruth);
    ASSERT_TRUE(conn->query("MATCH (u:User) WHERE u.name = 'Adam' SET u.age = NULL")->isSuccess());
    result = conn->query("MATCH (u:User) WHERE u.name='Adam' RETURN u.age");
    groundTruth = std::vector<std::string>{""};
    ASSERT_EQ(TestHelper::convertResultToString(*result, true /* check order */), groundTruth);
}
