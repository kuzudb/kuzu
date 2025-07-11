#pragma once

#include "c_api/kuzu.h"
#include "graph_test/base_graph_test.h"

namespace kuzu {
namespace testing {

// This class starts database in on-disk mode.
class APIDBTest : public BaseGraphTest {
public:
    void SetUp() override {
        BaseGraphTest::SetUp();
        createDBAndConn();
        initGraph();
    }
};

class CApiTest : public APIDBTest {
public:
    kuzu_database _database;
    kuzu_connection connection;

    void SetUp() override {
        APIDBTest::SetUp();
        auto* connCppPointer = conn.release();
        auto* databaseCppPointer = database.release();
        connection = kuzu_connection{connCppPointer};
        _database = kuzu_database{databaseCppPointer};
    }

    std::string getDatabasePath() { return databasePath; }

    kuzu_database* getDatabase() { return &_database; }

    kuzu_connection* getConnection() { return &connection; }

    void TearDown() override {
        kuzu_connection_destroy(&connection);
        kuzu_database_destroy(&_database);
        APIDBTest::TearDown();
    }
};

} // namespace testing
} // namespace kuzu
