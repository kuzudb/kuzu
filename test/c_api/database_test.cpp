#include "c_api/kuzu.h"
#include "graph_test/api_graph_test.h"
#include "gtest/gtest.h"

using namespace kuzu::main;
using namespace kuzu::testing;

class CApiDatabaseTest : public APIEmptyDBTest {
public:
    void SetUp() override {
        APIEmptyDBTest::SetUp();
        defaultSystemConfig = kuzu_default_system_config();

        // limit memory usage by keeping max number of threads small
        defaultSystemConfig.max_num_threads = 2;
    }

    kuzu_system_config defaultSystemConfig;
};

TEST_F(CApiDatabaseTest, CreationAndDestroy) {
    kuzu_database database;
    kuzu_state state;
    auto databasePathCStr = databasePath.c_str();
    auto systemConfig = defaultSystemConfig;
    state = kuzu_database_init(databasePathCStr, systemConfig, &database);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(database._database, nullptr);
    auto databaseCpp = static_cast<Database*>(database._database);
    ASSERT_NE(databaseCpp, nullptr);
    kuzu_database_destroy(&database);
}

TEST_F(CApiDatabaseTest, CreationReadOnly) {
    kuzu_database database;
    kuzu_connection connection;
    kuzu_query_result queryResult;
    kuzu_state state;
    auto databasePathCStr = databasePath.c_str();
    auto systemConfig = defaultSystemConfig;
    // First, create a read-write database.
    state = kuzu_database_init(databasePathCStr, systemConfig, &database);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(database._database, nullptr);
    auto databaseCpp = static_cast<Database*>(database._database);
    ASSERT_NE(databaseCpp, nullptr);
    kuzu_database_destroy(&database);
    // Now, access the same database read-only.
    systemConfig.read_only = true;
    state = kuzu_database_init(databasePathCStr, systemConfig, &database);
    if (databasePath == "" || databasePath == ":memory:") {
        ASSERT_EQ(state, KuzuError);
        ASSERT_EQ(database._database, nullptr);
        return;
    }
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(database._database, nullptr);
    databaseCpp = static_cast<Database*>(database._database);
    ASSERT_NE(databaseCpp, nullptr);
    // Try to write to the database.
    state = kuzu_connection_init(&database, &connection);
    ASSERT_EQ(state, KuzuSuccess);
    state = kuzu_connection_query(&connection,
        "CREATE NODE TABLE User(name STRING, age INT64, reg_date DATE, PRIMARY KEY (name))",
        &queryResult);
    ASSERT_EQ(state, KuzuError);
    ASSERT_FALSE(kuzu_query_result_is_success(&queryResult));
    kuzu_query_result_destroy(&queryResult);
    kuzu_connection_destroy(&connection);
    kuzu_database_destroy(&database);
}

TEST_F(CApiDatabaseTest, CreationInMemory) {
    kuzu_database database;
    kuzu_state state;
    auto databasePathCStr = (char*)"";
    state = kuzu_database_init(databasePathCStr, defaultSystemConfig, &database);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_database_destroy(&database);
    databasePathCStr = (char*)":memory:";
    state = kuzu_database_init(databasePathCStr, defaultSystemConfig, &database);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_database_destroy(&database);
}

#ifndef __WASM__ // home directory is not available in WASM
TEST_F(CApiDatabaseTest, CreationHomeDir) {
    kuzu_database database;
    kuzu_connection connection;
    kuzu_state state;
    auto databasePathCStr = (char*)"~/ku_test.db/";
    state = kuzu_database_init(databasePathCStr, defaultSystemConfig, &database);
    ASSERT_EQ(state, KuzuSuccess);
    state = kuzu_connection_init(&database, &connection);
    ASSERT_EQ(state, KuzuSuccess);
    auto homePath =
        getClientContext(*(Connection*)(connection._connection))->getClientConfig()->homeDirectory;
    kuzu_connection_destroy(&connection);
    kuzu_database_destroy(&database);
    std::filesystem::remove_all(homePath + "/ku_test.db");
}
#endif
