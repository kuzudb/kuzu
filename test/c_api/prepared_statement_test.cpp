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
    kuzu_prepared_statement preparedStatement;
    kuzu_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(preparedStatement._prepared_statement, nullptr);
    ASSERT_TRUE(kuzu_prepared_statement_is_success(&preparedStatement));
    kuzu_prepared_statement_destroy(&preparedStatement);

    query = "MATCH (a:personnnn) WHERE a.isStudent = $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(preparedStatement._prepared_statement, nullptr);
    ASSERT_FALSE(kuzu_prepared_statement_is_success(&preparedStatement));
    kuzu_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, GetErrorMessage) {
    kuzu_prepared_statement preparedStatement;
    kuzu_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(preparedStatement._prepared_statement, nullptr);
    ASSERT_EQ(kuzu_prepared_statement_get_error_message(&preparedStatement), nullptr);
    kuzu_prepared_statement_destroy(&preparedStatement);

    query = "MATCH (a:personnnn) WHERE a.isStudent = $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(preparedStatement._prepared_statement, nullptr);
    char* message = kuzu_prepared_statement_get_error_message(&preparedStatement);
    ASSERT_EQ(std::string(message), "Binder exception: Table personnnn does not exist.");
    kuzu_prepared_statement_destroy(&preparedStatement);
    kuzu_destroy_string(message);
}

TEST_F(CApiPreparedStatementTest, BindBool) {
    kuzu_prepared_statement preparedStatement;
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.isStudent = $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_EQ(kuzu_prepared_statement_bind_bool(&preparedStatement, "1", true), KuzuSuccess);
    state = kuzu_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 3);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindInt64) {
    kuzu_prepared_statement preparedStatement;
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.age > $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_EQ(kuzu_prepared_statement_bind_int64(&preparedStatement, "1", 30), KuzuSuccess);
    state = kuzu_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 4);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindInt32) {
    kuzu_prepared_statement preparedStatement;
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:movies) WHERE a.length > $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_EQ(kuzu_prepared_statement_bind_int32(&preparedStatement, "1", 200), KuzuSuccess);
    state = kuzu_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindInt16) {
    kuzu_prepared_statement preparedStatement;
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.length > $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_EQ(kuzu_prepared_statement_bind_int16(&preparedStatement, "1", 10), KuzuSuccess);
    state = kuzu_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindInt8) {
    kuzu_prepared_statement preparedStatement;
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.level > $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_EQ(kuzu_prepared_statement_bind_int8(&preparedStatement, "1", 3), KuzuSuccess);
    state = kuzu_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindUInt64) {
    kuzu_prepared_statement preparedStatement;
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.code > $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(kuzu_prepared_statement_bind_uint64(&preparedStatement, "1", 100), KuzuSuccess);
    state = kuzu_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindUInt32) {
    kuzu_prepared_statement preparedStatement;
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.temperature> $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_EQ(kuzu_prepared_statement_bind_uint32(&preparedStatement, "1", 10), KuzuSuccess);
    state = kuzu_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindUInt16) {
    kuzu_prepared_statement preparedStatement;
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.ulength> $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_EQ(kuzu_prepared_statement_bind_uint16(&preparedStatement, "1", 100), KuzuSuccess);
    state = kuzu_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindUInt8) {
    kuzu_prepared_statement preparedStatement;
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    auto query =
        "MATCH (a:person) -[s:studyAt]-> (b:organisation) WHERE s.ulevel> $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_EQ(kuzu_prepared_statement_bind_uint8(&preparedStatement, "1", 14), KuzuSuccess);
    state = kuzu_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 2);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindDouble) {
    kuzu_prepared_statement preparedStatement;
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.eyeSight > $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_EQ(kuzu_prepared_statement_bind_double(&preparedStatement, "1", 4.5), KuzuSuccess);
    state = kuzu_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 7);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindFloat) {
    kuzu_prepared_statement preparedStatement;
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.height < $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_EQ(kuzu_prepared_statement_bind_float(&preparedStatement, "1", 1.0), KuzuSuccess);
    state = kuzu_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 1);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindString) {
    kuzu_prepared_statement preparedStatement;
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.fName = $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_prepared_statement_is_success(&preparedStatement));
    ASSERT_EQ(kuzu_prepared_statement_bind_string(&preparedStatement, "1", "Alice"), KuzuSuccess);
    state = kuzu_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 1);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindDate) {
    kuzu_prepared_statement preparedStatement;
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.birthdate > $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_prepared_statement_is_success(&preparedStatement));
    auto date = kuzu_date_t{0};
    ASSERT_EQ(kuzu_prepared_statement_bind_date(&preparedStatement, "1", date), KuzuSuccess);
    state = kuzu_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 4);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindTimestamp) {
    kuzu_prepared_statement preparedStatement;
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.registerTime > $1 and cast(a.registerTime, "
                 "\"timestamp_ns\") > $2 and cast(a.registerTime, \"timestamp_ms\") > "
                 "$3 and cast(a.registerTime, \"timestamp_sec\") > $4 and cast(a.registerTime, "
                 "\"timestamp_tz\") > $5 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_prepared_statement_is_success(&preparedStatement));
    auto timestamp = kuzu_timestamp_t{0};
    auto timestamp_ns = kuzu_timestamp_ns_t{1};
    auto timestamp_ms = kuzu_timestamp_ms_t{2};
    auto timestamp_sec = kuzu_timestamp_sec_t{3};
    auto timestamp_tz = kuzu_timestamp_tz_t{4};
    ASSERT_EQ(kuzu_prepared_statement_bind_timestamp(&preparedStatement, "1", timestamp),
        KuzuSuccess);
    ASSERT_EQ(kuzu_prepared_statement_bind_timestamp_ns(&preparedStatement, "2", timestamp_ns),
        KuzuSuccess);
    ASSERT_EQ(kuzu_prepared_statement_bind_timestamp_ms(&preparedStatement, "3", timestamp_ms),
        KuzuSuccess);
    ASSERT_EQ(kuzu_prepared_statement_bind_timestamp_sec(&preparedStatement, "4", timestamp_sec),
        KuzuSuccess);
    ASSERT_EQ(kuzu_prepared_statement_bind_timestamp_tz(&preparedStatement, "5", timestamp_tz),
        KuzuSuccess);
    state = kuzu_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 7);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindInteval) {
    kuzu_prepared_statement preparedStatement;
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.lastJobDuration > $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_prepared_statement_is_success(&preparedStatement));
    auto interval = kuzu_interval_t{0, 0, 0};
    ASSERT_EQ(kuzu_prepared_statement_bind_interval(&preparedStatement, "1", interval),
        KuzuSuccess);
    state = kuzu_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 8);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&preparedStatement);
}

TEST_F(CApiPreparedStatementTest, BindValue) {
    kuzu_prepared_statement preparedStatement;
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    auto query = "MATCH (a:person) WHERE a.registerTime > $1 RETURN COUNT(*)";
    state = kuzu_connection_prepare(connection, query, &preparedStatement);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_prepared_statement_is_success(&preparedStatement));
    auto timestamp = kuzu_timestamp_t{0};
    auto timestampValue = kuzu_value_create_timestamp(timestamp);
    ASSERT_EQ(kuzu_prepared_statement_bind_value(&preparedStatement, "1", timestampValue),
        KuzuSuccess);
    kuzu_value_destroy(timestampValue);
    state = kuzu_connection_execute(connection, &preparedStatement, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_NE(result._query_result, nullptr);
    ASSERT_EQ(kuzu_query_result_get_num_tuples(&result), 1);
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 1);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    auto resultCpp = static_cast<QueryResult*>(result._query_result);
    auto tuple = resultCpp->getNext();
    auto value = tuple->getValue(0)->getValue<int64_t>();
    ASSERT_EQ(value, 7);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&preparedStatement);
}
