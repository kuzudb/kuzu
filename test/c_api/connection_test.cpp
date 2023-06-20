#include "c_api_test/c_api_test.h"

using ::testing::Test;
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

TEST_F(CApiConnectionTest, TransactionModes) {
    auto connection = getConnection();

    // Test initially connections are in AUTO_COMMIT mode.
    ASSERT_EQ(Connection::ConnectionTransactionMode::AUTO_COMMIT,
        getTransactionMode(*(Connection*)connection->_connection));
    // Test beginning a transaction (first in read only mode) sets mode to MANUAL automatically.
    kuzu_connection_begin_read_only_transaction(connection);
    ASSERT_EQ(Connection::ConnectionTransactionMode::MANUAL,
        getTransactionMode(*(Connection*)connection->_connection));
    // Test commit automatically switches the mode to AUTO_COMMIT read transaction
    kuzu_connection_commit(connection);
    ASSERT_EQ(Connection::ConnectionTransactionMode::AUTO_COMMIT,
        getTransactionMode(*(Connection*)connection->_connection));

    kuzu_connection_begin_read_only_transaction(connection);
    ASSERT_EQ(Connection::ConnectionTransactionMode::MANUAL,
        getTransactionMode(*(Connection*)connection->_connection));
    // Test rollback automatically switches the mode to AUTO_COMMIT for read transaction
    kuzu_connection_rollback(connection);
    ASSERT_EQ(Connection::ConnectionTransactionMode::AUTO_COMMIT,
        getTransactionMode(*(Connection*)connection->_connection));

    // Test beginning a transaction (now in write mode) sets mode to MANUAL automatically.
    kuzu_connection_begin_write_transaction(connection);
    ASSERT_EQ(Connection::ConnectionTransactionMode::MANUAL,
        getTransactionMode(*(Connection*)connection->_connection));
    // Test commit automatically switches the mode to AUTO_COMMIT for write transaction
    kuzu_connection_commit(connection);
    ASSERT_EQ(Connection::ConnectionTransactionMode::AUTO_COMMIT,
        getTransactionMode(*(Connection*)connection->_connection));

    // Test beginning a transaction (now in write mode) sets mode to MANUAL automatically.
    kuzu_connection_begin_write_transaction(connection);
    ASSERT_EQ(Connection::ConnectionTransactionMode::MANUAL,
        getTransactionMode(*(Connection*)connection->_connection));
    // Test rollback automatically switches the mode to AUTO_COMMIT write transaction
    kuzu_connection_rollback(connection);
    ASSERT_EQ(Connection::ConnectionTransactionMode::AUTO_COMMIT,
        getTransactionMode(*(Connection*)connection->_connection));
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
    kuzu_prepared_statement_bind_bool(statement, (char*)"1", true);
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

TEST_F(CApiConnectionTest, GetNodeTableNames) {
    auto connection = getConnection();
    auto result = kuzu_connection_get_node_table_names(connection);
    ASSERT_NE(result, nullptr);
    auto resultString = std::string(result);
    ASSERT_EQ(resultString, "Node tables: \n"
                            "\tmovies\n"
                            "\torganisation\n"
                            "\tperson\n");
    free(result);
}

TEST_F(CApiConnectionTest, GetRelTableNames) {
    auto connection = getConnection();
    auto result = kuzu_connection_get_rel_table_names(connection);
    ASSERT_NE(result, nullptr);
    auto resultString = std::string(result);
    ASSERT_EQ(resultString, "Rel tables: \n"
                            "\tknows\n"
                            "\tmarries\n"
                            "\tmeets\n"
                            "\tstudyAt\n"
                            "\tworkAt\n");
    free(result);
}

TEST_F(CApiConnectionTest, GetNodePropertyNames) {
    auto connection = getConnection();
    auto result = kuzu_connection_get_node_property_names(connection, "movies");
    ASSERT_NE(result, nullptr);
    auto resultString = std::string(result);
    ASSERT_EQ(resultString,
        "movies properties: \n"
        "\tname STRING(PRIMARY KEY)\n"
        "\tlength INT32\n"
        "\tnote STRING\n"
        "\tdescription STRUCT(RATING:DOUBLE, VIEWS:INT64, RELEASE:TIMESTAMP, FILM:DATE)\n"
        "\tcontent BLOB\n");
    free(result);
}

TEST_F(CApiConnectionTest, GetRelPropertyNames) {
    auto connection = getConnection();
    auto result = kuzu_connection_get_rel_property_names(connection, "meets");
    ASSERT_NE(result, nullptr);
    auto resultString = std::string(result);
    ASSERT_EQ(resultString, "meets src node: person\n"
                            "meets dst node: person\n"
                            "meets properties: \n"
                            "\tlocation FLOAT[2]\n"
                            "\ttimes INT32\n"
                            "\tdata BLOB\n");
    free(result);
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
    // Interrupt the query after 100ms
    std::thread t([&connection]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
        kuzu_connection_interrupt(connection);
    });
    t.detach();
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
