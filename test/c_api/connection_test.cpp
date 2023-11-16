#include <thread>

#include "c_api_test/c_api_test.h"

using ::testing::Test;
using namespace kuzu::main;
using namespace kuzu::testing;

class CApiConnectionTest : public CApiTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

TEST_F(CApiConnectionTest, CreationAndDestroy) {
    auto _database = getDatabase();
    auto connection = kuzu_connection_init(_database);
    ASSERT_NE(connection, nullptr);
    ASSERT_NE(connection->_connection, nullptr);
    auto connectionCpp = static_cast<Connection*>(connection->_connection);
    ASSERT_NE(connectionCpp, nullptr);
    kuzu_connection_destroy(connection);
}

TEST_F(CApiConnectionTest, CreationAndDestroyWithNullDatabase) {
    auto connection = kuzu_connection_init(nullptr);
    ASSERT_EQ(connection, nullptr);
}

TEST_F(CApiConnectionTest, Query) {
    auto connection = getConnection();
    auto query = "MATCH (a:person) RETURN a.fName";
    auto result = kuzu_connection_query(connection, query);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    ASSERT_NE(resultCpp, nullptr);
    ASSERT_TRUE(resultCpp->isSuccess());
    ASSERT_TRUE(resultCpp->hasNext());
    ASSERT_EQ(resultCpp->getErrorMessage(), "");
    ASSERT_EQ(resultCpp->getNumTuples(), 8);
    ASSERT_EQ(resultCpp->getNumColumns(), 1);
    ASSERT_EQ(resultCpp->getColumnNames()[0], "a.fName");
    kuzu_query_result_destroy(result);
}

TEST_F(CApiConnectionTest, SetGetMaxNumThreadForExec) {
    auto connection = getConnection();
    kuzu_connection_set_max_num_thread_for_exec(connection, 4);
    auto connectionCpp = static_cast<Connection*>(connection->_connection);
    ASSERT_EQ(connectionCpp->getMaxNumThreadForExec(), 4);
    auto maxNumThreadForExec = kuzu_connection_get_max_num_thread_for_exec(connection);
    ASSERT_EQ(maxNumThreadForExec, 4);
    kuzu_connection_set_max_num_thread_for_exec(connection, 8);
    ASSERT_EQ(connectionCpp->getMaxNumThreadForExec(), 8);
    maxNumThreadForExec = kuzu_connection_get_max_num_thread_for_exec(connection);
    ASSERT_EQ(maxNumThreadForExec, 8);
}

TEST_F(CApiConnectionTest, Prepare) {
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
    auto statement = kuzu_connection_prepare(connection, query);
    ASSERT_NE(statement, nullptr);
    ASSERT_NE(statement->_prepared_statement, nullptr);
    ASSERT_NE(statement->_bound_values, nullptr);
    auto statementCpp = static_cast<PreparedStatement*>(statement->_prepared_statement);
    ASSERT_NE(statementCpp, nullptr);
    auto connectionCpp = static_cast<Connection*>(connection->_connection);
    auto result = connectionCpp->execute(statementCpp, std::make_pair(std::string("1"), true));
    ASSERT_TRUE(result->isSuccess());
    ASSERT_TRUE(result->hasNext());
    ASSERT_EQ(result->getErrorMessage(), "");
    ASSERT_EQ(result->getNumTuples(), 1);
    auto tuple = result->getNext();
    ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 3);
    kuzu_prepared_statement_destroy(statement);
}

TEST_F(CApiConnectionTest, Execute) {
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
    auto statement = kuzu_connection_prepare(connection, query);
    kuzu_prepared_statement_bind_bool(statement, "1", true);
    auto result = kuzu_connection_execute(connection, statement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    ASSERT_NE(resultCpp, nullptr);
    ASSERT_TRUE(resultCpp->isSuccess());
    ASSERT_TRUE(resultCpp->hasNext());
    ASSERT_EQ(resultCpp->getErrorMessage(), "");
    ASSERT_EQ(resultCpp->getNumTuples(), 1);
    auto tuple = resultCpp->getNext();
    ASSERT_EQ(tuple->getValue(0)->getValue<int64_t>(), 3);
    kuzu_prepared_statement_destroy(statement);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiConnectionTest, QueryTimeout) {
    auto connection = getConnection();
    kuzu_connection_set_query_timeout(connection, 1);
    auto result = kuzu_connection_query(
        connection, "MATCH (a:person)-[:knows*1..28]->(b:person) RETURN COUNT(*);");
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    ASSERT_NE(resultCpp, nullptr);
    ASSERT_FALSE(resultCpp->isSuccess());
    ASSERT_EQ(resultCpp->getErrorMessage(), "Interrupted.");
    kuzu_query_result_destroy(result);
}

TEST_F(CApiConnectionTest, Interrupt) {
    auto connection = getConnection();
    bool finished = false;

    // Interrupt the query after 100ms
    // This may happen too early, so try again until the query function finishes.
    std::thread t([&connection, &finished]() {
        do {
            std::this_thread::sleep_for(std::chrono::milliseconds(100));
            kuzu_connection_interrupt(connection);
        } while (!finished);
    });
    auto result = kuzu_connection_query(
        connection, "MATCH (a:person)-[:knows*1..28]->(b:person) RETURN COUNT(*);");
    finished = true;
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    ASSERT_NE(resultCpp, nullptr);
    ASSERT_FALSE(resultCpp->isSuccess());
    ASSERT_EQ(resultCpp->getErrorMessage(), "Interrupted.");
    kuzu_query_result_destroy(result);
    t.join();
}
