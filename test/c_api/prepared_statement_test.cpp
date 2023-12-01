#include "c_api_test/c_api_test.h"

using namespace kuzu::main;
using namespace kuzu::testing;

class CApiPreparedStatementTest : public CApiTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

TEST_F(CApiPreparedStatementTest, IsSuccess) {
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    ASSERT_NE(preparedStatement, nullptr);
    ASSERT_NE(preparedStatement->_prepared_statement, nullptr);
    ASSERT_TRUE(kuzu_prepared_statement_is_success(preparedStatement));
    kuzu_prepared_statement_destroy(preparedStatement);

    query = "MATCH (a:personnnn) WHERE a.isStudent = $1 RETURN COUNT(*)";
    preparedStatement = kuzu_connection_prepare(connection, query);
    ASSERT_NE(preparedStatement, nullptr);
    ASSERT_NE(preparedStatement->_prepared_statement, nullptr);
    ASSERT_FALSE(kuzu_prepared_statement_is_success(preparedStatement));
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, GetErrorMessage) {
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    ASSERT_NE(preparedStatement, nullptr);
    ASSERT_NE(preparedStatement->_prepared_statement, nullptr);
    auto message = kuzu_prepared_statement_get_error_message(preparedStatement);
    ASSERT_EQ(message, nullptr);
    kuzu_prepared_statement_destroy(preparedStatement);

    query = "MATCH (a:personnnn) WHERE a.isStudent = $1 RETURN COUNT(*)";
    preparedStatement = kuzu_connection_prepare(connection, query);
    ASSERT_NE(preparedStatement, nullptr);
    ASSERT_NE(preparedStatement->_prepared_statement, nullptr);
    message = kuzu_prepared_statement_get_error_message(preparedStatement);
    ASSERT_NE(message, nullptr);
    ASSERT_EQ(std::string(message), "Binder exception: Table personnnn does not exist.");
    kuzu_prepared_statement_destroy(preparedStatement);
    free(message);
}

TEST_F(CApiPreparedStatementTest, AllowActiveTransaction) {
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    ASSERT_NE(preparedStatement, nullptr);
    ASSERT_NE(preparedStatement->_prepared_statement, nullptr);
    ASSERT_TRUE(kuzu_prepared_statement_is_success(preparedStatement));
    ASSERT_TRUE(kuzu_prepared_statement_allow_active_transaction(preparedStatement));
    kuzu_prepared_statement_destroy(preparedStatement);

    query = "create node table npytable (id INT64,i64 INT64[12],PRIMARY KEY(id));";
    preparedStatement = kuzu_connection_prepare(connection, query);
    ASSERT_NE(preparedStatement, nullptr);
    ASSERT_NE(preparedStatement->_prepared_statement, nullptr);
    ASSERT_TRUE(kuzu_prepared_statement_is_success(preparedStatement));
    ASSERT_FALSE(kuzu_prepared_statement_allow_active_transaction(preparedStatement));
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindBool) {
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    kuzu_prepared_statement_bind_bool(preparedStatement, "1", true);
    auto result = kuzu_connection_execute(connection, preparedStatement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 3);
    kuzu_query_result_destroy(result);
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindInt64) {
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.age > $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    kuzu_prepared_statement_bind_int64(preparedStatement, "1", 30);
    auto result = kuzu_connection_execute(connection, preparedStatement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 4);
    kuzu_query_result_destroy(result);
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindInt32) {
    auto connection = getConnection();
    auto query = "MATCH (a:movies) WHERE a.length > $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    kuzu_prepared_statement_bind_int32(preparedStatement, "1", 200);
    auto result = kuzu_connection_execute(connection, preparedStatement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    kuzu_query_result_destroy(result);
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindInt16) {
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.length > $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    kuzu_prepared_statement_bind_int16(preparedStatement, "1", 10);
    auto result = kuzu_connection_execute(connection, preparedStatement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    kuzu_query_result_destroy(result);
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindInt8) {
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.level > $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    kuzu_prepared_statement_bind_int8(preparedStatement, "1", 3);
    auto result = kuzu_connection_execute(connection, preparedStatement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    kuzu_query_result_destroy(result);
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindUInt64) {
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.code > $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    kuzu_prepared_statement_bind_uint64(preparedStatement, "1", 100);
    auto result = kuzu_connection_execute(connection, preparedStatement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    kuzu_query_result_destroy(result);
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindUInt32) {
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.temprature> $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    kuzu_prepared_statement_bind_uint32(preparedStatement, "1", 10);
    auto result = kuzu_connection_execute(connection, preparedStatement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    kuzu_query_result_destroy(result);
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindUInt16) {
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.ulength> $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    kuzu_prepared_statement_bind_uint16(preparedStatement, "1", 100);
    auto result = kuzu_connection_execute(connection, preparedStatement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    kuzu_query_result_destroy(result);
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindUInt8) {
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.ulevel> $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    kuzu_prepared_statement_bind_uint8(preparedStatement, "1", 14);
    auto result = kuzu_connection_execute(connection, preparedStatement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    kuzu_query_result_destroy(result);
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindDouble) {
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.eyeSight > $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    kuzu_prepared_statement_bind_double(preparedStatement, "1", 4.5);
    auto result = kuzu_connection_execute(connection, preparedStatement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 7);
    kuzu_query_result_destroy(result);
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindFloat) {
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.height < $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    kuzu_prepared_statement_bind_float(preparedStatement, "1", 1.0);
    auto result = kuzu_connection_execute(connection, preparedStatement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 1);
    kuzu_query_result_destroy(result);
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindString) {
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.fName = $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    ASSERT_TRUE(kuzu_prepared_statement_is_success(preparedStatement));
    kuzu_prepared_statement_bind_string(preparedStatement, "1", "Alice");
    auto result = kuzu_connection_execute(connection, preparedStatement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 1);
    kuzu_query_result_destroy(result);
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindDate) {
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.birthdate > $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    ASSERT_TRUE(kuzu_prepared_statement_is_success(preparedStatement));
    auto date = kuzu_date_t{0};
    kuzu_prepared_statement_bind_date(preparedStatement, "1", date);
    auto result = kuzu_connection_execute(connection, preparedStatement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 4);
    kuzu_query_result_destroy(result);
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindTimestamp) {
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.registerTime > $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    ASSERT_TRUE(kuzu_prepared_statement_is_success(preparedStatement));
    auto timestamp = kuzu_timestamp_t{0};
    kuzu_prepared_statement_bind_timestamp(preparedStatement, "1", timestamp);
    auto result = kuzu_connection_execute(connection, preparedStatement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 7);
    kuzu_query_result_destroy(result);
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindInteval) {
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.lastJobDuration > $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    ASSERT_TRUE(kuzu_prepared_statement_is_success(preparedStatement));
    auto interval = kuzu_interval_t{0, 0, 0};
    kuzu_prepared_statement_bind_interval(preparedStatement, "1", interval);
    auto result = kuzu_connection_execute(connection, preparedStatement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 8);
    kuzu_query_result_destroy(result);
    kuzu_prepared_statement_destroy(preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindValue) {
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.registerTime > $1 RETURN COUNT(*)";
    auto preparedStatement = kuzu_connection_prepare(connection, query);
    ASSERT_TRUE(kuzu_prepared_statement_is_success(preparedStatement));
    auto timestamp = kuzu_timestamp_t{0};
    auto timestampValue = kuzu_value_create_timestamp(timestamp);
    kuzu_prepared_statement_bind_value(preparedStatement, "1", timestampValue);
    kuzu_value_destroy(timestampValue);
    auto result = kuzu_connection_execute(connection, preparedStatement);
    ASSERT_NE(result, nullptr);
    ASSERT_NE(result->_query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto resultCpp = static_cast<QueryResult*>(result->_query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 7);
    kuzu_query_result_destroy(result);
    kuzu_prepared_statement_destroy(preparedStatement);
}
