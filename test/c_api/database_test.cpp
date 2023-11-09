#include "c_api/kuzu.h"
#include "graph_test/api_graph_test.h"
#include "gtest/gtest.h"

using namespace kuzu::main;
using namespace kuzu::testing;

class CApiDatabaseTest : public APIEmptyDBTest {
public:
    void SetUp() override { APIEmptyDBTest::SetUp(); }
};

TEST_F(CApiDatabaseTest, CreationAndDestroy) {
    auto databasePathCStr = databasePath.c_str();
    auto systemConfig = kuzu_default_system_config();
    auto database = kuzu_database_init(databasePathCStr, systemConfig);
    ASSERT_NE(database, nullptr);
    ASSERT_NE(database->_database, nullptr);
    auto databaseCpp = static_cast<Database*>(database->_database);
    ASSERT_NE(databaseCpp, nullptr);
    kuzu_database_destroy(database);
}

TEST_F(CApiDatabaseTest, CreationReadOnly) {
// TODO: Enable this test on Windows when the read-only mode is implemented.
#ifdef _WIN32
    return;
#endif
    auto databasePathCStr = databasePath.c_str();
    auto systemConfig = kuzu_default_system_config();
    // First, create a read-write database.
    auto database = kuzu_database_init(databasePathCStr, systemConfig);
    ASSERT_NE(database, nullptr);
    ASSERT_NE(database->_database, nullptr);
    auto databaseCpp = static_cast<Database*>(database->_database);
    ASSERT_NE(databaseCpp, nullptr);
    kuzu_database_destroy(database);
    // Now, access the same database read-only.
    systemConfig.read_only = true;
    database = kuzu_database_init(databasePathCStr, systemConfig);
    ASSERT_NE(database, nullptr);
    ASSERT_NE(database->_database, nullptr);
    databaseCpp = static_cast<Database*>(database->_database);
    ASSERT_NE(databaseCpp, nullptr);
    // Try to write to the database.
    auto connection = kuzu_connection_init(database);
    ASSERT_NE(connection, nullptr);
    auto queryResult = kuzu_connection_query(connection,
        "CREATE NODE TABLE User(name STRING, age INT64, reg_date DATE, PRIMARY KEY (name))");
    ASSERT_EQ(queryResult, nullptr);
    kuzu_connection_destroy(connection);
    kuzu_database_destroy(database);
}

TEST_F(CApiDatabaseTest, CreationInvalidPath) {
    auto databasePathCStr = (char*)"";
    auto database = kuzu_database_init(databasePathCStr, kuzu_default_system_config());
    ASSERT_EQ(database, nullptr);
}
