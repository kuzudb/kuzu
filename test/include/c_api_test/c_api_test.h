#pragma once

#include <cstring>

#include "c_api/kuzu.h"
#include "common/file_utils.h"
#include "graph_test/graph_test.h"
#include "gtest/gtest.h"
#include "main/kuzu.h"
#include "parser/parser.h"
#include "planner/logical_plan/logical_plan_util.h"
#include "planner/planner.h"
#include "test_helper/test_helper.h"

using ::testing::Test;
using namespace kuzu::testing;

class CApiTest : public DBTest {
public:
    kuzu_database* _database;
    kuzu_connection* connection;

    void SetUp() override {
        DBTest::SetUp();
        // In C API tests, we don't use the database and connection created by DBTest because
        // they are not C++ objects.
        conn.reset();
        database.reset();
        auto databasePath = getDatabasePath();
        auto databasePathCStr = databasePath.c_str();
        _database = kuzu_database_init(databasePathCStr, 0);
        connection = kuzu_connection_init(_database);
    }

    std::string getDatabasePath() { return databasePath; }

    kuzu_database* getDatabase() const { return _database; }

    kuzu_connection* getConnection() const { return connection; }

    void TearDown() override {
        kuzu_connection_destroy(connection);
        kuzu_database_destroy(_database);
        DBTest::TearDown();
    }
};
