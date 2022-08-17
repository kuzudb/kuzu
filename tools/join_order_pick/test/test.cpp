#include "test/test_utility/include/test_helper.h"
#include "tools/join_order_pick/include/jo_connection.h"

using namespace graphflow::main;
using namespace graphflow::testing;
using ::testing::Test;

class JoinOrderPickTest : public Test {
public:
    void SetUp() override {
        auto systemConfig = make_unique<SystemConfig>();
        auto databaseConfig = make_unique<DatabaseConfig>(TEMP_DIR);
        database = make_unique<Database>(*databaseConfig, *systemConfig);
        conn = make_unique<JOConnection>(database.get());
        initDB();
    }

    void TearDown() override { FileUtils::removeDir(TEMP_DIR); }

    void initDB() {
        auto regularConnection = make_unique<Connection>(database.get());
        regularConnection->query(
            "CREATE NODE TABLE person (ID INT64, fName STRING, gender INT64, isStudent BOOLEAN, "
            "isWorker BOOLEAN, "
            "age INT64, eyeSight DOUBLE, birthdate DATE, registerTime TIMESTAMP, lastJobDuration "
            "INTERVAL, workedHours INT64[], usedNames STRING[], courseScoresPerTerm INT64[][], "
            "PRIMARY "
            "KEY (ID))");
        regularConnection->query("COPY person FROM \"dataset/tinysnb/vPerson.csv\"");
        regularConnection->query(
            "CREATE REL knows (FROM person TO person, date DATE, meetTime TIMESTAMP, "
            "validInterval INTERVAL, comments STRING[], MANY_MANY)");
        regularConnection->query("COPY knows FROM \"dataset/tinysnb/eKnows.csv\"");
    }

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
