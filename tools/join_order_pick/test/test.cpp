#include "gtest/gtest.h"
#include "tools/join_order_pick/include/jo_connection.h"

#include "src/loader/include/graph_loader.h"
#include "src/main/include/graphflowdb.h"

using namespace graphflow::loader;
using namespace graphflow::main;
using ::testing::Test;

class JoinOrderPickTest : public Test {
public:
    void SetUp() override {
        auto systemConfig = make_unique<SystemConfig>();
        auto databaseConfig = make_unique<DatabaseConfig>(TEMP_DIR);
        GraphLoader graphLoader("dataset/tinysnb/", TEMP_DIR, systemConfig->maxNumThreads,
            systemConfig->defaultPageBufferPoolSize, systemConfig->largePageBufferPoolSize);
        graphLoader.loadGraph();
        database = make_unique<Database>(*databaseConfig, *systemConfig);
        conn = make_unique<JOConnection>(database.get());
    }

    void TearDown() override { FileUtils::removeDir(TEMP_DIR); }

public:
    static constexpr char TEMP_DIR[] = "./temp/";
    unique_ptr<Database> database;
    unique_ptr<JOConnection> conn;
};

TEST_F(JoinOrderPickTest, OneHop) {
    auto queryStr = "MATCH (a:person)-[:knows]->(b:person) RETURN a.ID";
    ASSERT_TRUE(conn->query(queryStr, "E(b)S(a)")->isSuccess());
    ASSERT_TRUE(conn->query(queryStr, "E(a)S(b)")->isSuccess());
    ASSERT_TRUE(conn->query(queryStr, "HJ(b){E(b)S(a)}{S(b)}")->isSuccess());
}

TEST_F(JoinOrderPickTest, TwoHop) {
    auto queryStr = "MATCH (a:person)-[:knows]->(b:person)-[:knows]->(c:person) RETURN a.ID";
    ASSERT_TRUE(conn->query(queryStr, "HJ(c){S(c)}{HJ(a){E(a)E(c)S(b)}{S(a)}}")->isSuccess());
}
