#include "c_api/kuzu.h"
#include "graph_test/base_graph_test.h"
#include "gtest/gtest.h"

using namespace kuzu::main;
using namespace kuzu::testing;

// This class starts database without initializing graph.
class APIEmptyDBTest : public BaseGraphTest {
    std::string getInputDir() override { KU_UNREACHABLE; }
};

class CApiDatabaseTest : public APIEmptyDBTest {
public:
    void SetUp() override {
        APIEmptyDBTest::SetUp();
        defaultSystemConfig = kuzu_default_system_config();

        // limit memory usage by keeping max number of threads small
        defaultSystemConfig.max_num_threads = 2;
        auto maxDBSizeEnv = TestHelper::getSystemEnv("MAX_DB_SIZE");
        if (!maxDBSizeEnv.empty()) {
            defaultSystemConfig.max_db_size = std::stoull(maxDBSizeEnv);
        }
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
    auto databasePathCStr = (char*)"~/ku_test.db";
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

TEST_F(CApiDatabaseTest, CloseQueryResultAndConnectionAfterDatabaseDestroy) {
    kuzu_database database;
    auto databasePathCStr = (char*)":memory:";
    auto systemConfig = kuzu_default_system_config();
    systemConfig.buffer_pool_size = 10 * 1024 * 1024; // 10MB
    systemConfig.max_db_size = 1 << 30;               // 1GB
    systemConfig.max_num_threads = 2;
    kuzu_state state = kuzu_database_init(databasePathCStr, systemConfig, &database);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(database._database, nullptr);
    kuzu_connection conn;
    kuzu_query_result queryResult;
    state = kuzu_connection_init(&database, &conn);
    ASSERT_EQ(state, KuzuSuccess);
    state = kuzu_connection_query(&conn, "RETURN 1+1", &queryResult);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&queryResult));
    kuzu_flat_tuple tuple;
    kuzu_state resultState = kuzu_query_result_get_next(&queryResult, &tuple);
    ASSERT_EQ(resultState, KuzuSuccess);
    kuzu_value value;
    kuzu_state valueState = kuzu_flat_tuple_get_value(&tuple, 0, &value);
    ASSERT_EQ(valueState, KuzuSuccess);
    int64_t valueInt = INT64_MAX;
    kuzu_state valueIntState = kuzu_value_get_int64(&value, &valueInt);
    ASSERT_EQ(valueIntState, KuzuSuccess);
    ASSERT_EQ(valueInt, 2);
    // Destroy database first, this should not crash
    kuzu_database_destroy(&database);
    // Call kuzu_connection_query should not crash, but return an error
    state = kuzu_connection_query(&conn, "RETURN 1+1", &queryResult);
    ASSERT_EQ(state, KuzuError);
    // Call kuzu_query_result_get_next should not crash, but return an error
    resultState = kuzu_query_result_get_next(&queryResult, &tuple);
    ASSERT_EQ(resultState, KuzuError);
    // Now destroy everything, this should not crash
    kuzu_query_result_destroy(&queryResult);
    kuzu_connection_destroy(&conn);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&tuple);
}

TEST_F(CApiDatabaseTest, UseConnectionAfterDatabaseDestroy) {
    kuzu_database db;
    kuzu_connection conn;
    kuzu_query_result result;

    auto systemConfig = kuzu_default_system_config();
    systemConfig.buffer_pool_size = 10 * 1024 * 1024; // 10MB
    systemConfig.max_db_size = 1 << 30;               // 1GB
    systemConfig.max_num_threads = 2;
    auto state = kuzu_database_init("", systemConfig, &db);
    ASSERT_EQ(state, KuzuSuccess);
    state = kuzu_connection_init(&db, &conn);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_database_destroy(&db);
    state = kuzu_connection_query(&conn, "RETURN 0", &result);
    ASSERT_EQ(state, KuzuError);

    kuzu_connection_destroy(&conn);
}
