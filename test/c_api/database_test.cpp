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

TEST_F(CApiDatabaseTest, CreationHomeDir1) {
    createDBAndConn();
    printf("%s",
        conn->query("LOAD EXTENSION "
                    "'/Users/z473chen/Desktop/code/kuzu/extension/fts/build/libfts.kuzu_extension'")
            ->toString()
            .c_str());
    printf("%s", conn->query("create node table person (id int64, content string, author string, "
                             "primary key(id));")
                     ->toString()
                     .c_str());
    printf("%s",
        conn->query("create (p:person {id: 5, content: 'monster hero', author: 'monster'})")
            ->toString()
            .c_str());
    printf("%s",
        conn->query("create (p:person {id: 20, content: 'beats hero', author: 'monster beats'})")
            ->toString()
            .c_str());
    printf("%s", conn->query("create (p:person {id: 25, content: 'beats hero', author: 'beats'})")
                     ->toString()
                     .c_str());
    printf("%s",
        conn->query("CALL create_fts_index('person', 'test', ['content', 'author']) RETURN *")
            ->toString()
            .c_str());
//    printf("%s", conn->query("explain PROJECT GRAPH PK (person_dict, person_docs, person_terms) "
//                             "MATCH (a:person_dict) "
//                             "WHERE list_contains(['monster'], a.term) "
//                             "CALL FTS(PK, a) "
//                             "RETURN _node,score")
//                     ->toString()
//                     .c_str());
    printf("%s", conn->query(" PROJECT GRAPH PK (person_dict, person_docs, person_terms) "
                             "UNWIND tokenize('monster') AS tk "
                             "WITH collect(stem(tk, 'porter')) AS tokens "
                             "MATCH (a:person_dict) "
                             "WHERE list_contains(tokens, a.term) "
                             "CALL FTS(PK, a) "
                             "MATCH (p:person) "
                             "WHERE _node.offset = offset(id(p)) "
                             "RETURN p, score")
                     ->toString()
                     .c_str());
    //
    //    printf("%s", conn->query("CALL query_fts_index('person', 'test', 'monster') "
    //                             "return *;")
    //                     ->toString()
    //                     .c_str());
}
