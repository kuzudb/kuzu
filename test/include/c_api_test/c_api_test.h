#pragma once

#include "c_api/kuzu.h"
#include "graph_test/api_graph_test.h"

namespace kuzu {
namespace testing {

class CApiTest : public APIDBTest {
public:
    kuzu_database* _database;
    kuzu_connection* connection;

    void SetUp() override {
        APIDBTest::SetUp();
        // In C API tests, we don't use the database and connection created by DBTest because
        // they are not C++ objects.
        conn.reset();
        database.reset();
        auto databasePath = getDatabasePath();
        auto databasePathCStr = databasePath.c_str();
        auto systemConfig = kuzu_default_system_config();
        systemConfig.buffer_pool_size = 512 * 1024 * 1024;
        _database = kuzu_database_init(databasePathCStr, systemConfig);
        connection = kuzu_connection_init(_database);
    }

    std::string getDatabasePath() { return databasePath; }

    kuzu_database* getDatabase() const { return _database; }

    kuzu_connection* getConnection() const { return connection; }

    void TearDown() override {
        kuzu_connection_destroy(connection);
        kuzu_database_destroy(_database);
        APIDBTest::TearDown();
    }
};

} // namespace testing
} // namespace kuzu
