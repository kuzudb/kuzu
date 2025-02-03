#include <fstream>

#include "c_api_test/c_api_test.h"

using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::processor;
using namespace kuzu::testing;

class CApiQueryResultTest : public CApiTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

static kuzu_value* copy_flat_tuple(kuzu_flat_tuple* tuple, uint32_t tupleLen) {
    kuzu_value* ret = (kuzu_value*)malloc(sizeof(kuzu_value) * tupleLen);
    for (uint32_t i = 0; i < tupleLen; i++) {
        kuzu_flat_tuple_get_value(tuple, i, &ret[i]);
    }
    return ret;
}

TEST_F(CApiQueryResultTest, GetNextExample) {
    auto conn = getConnection();

    kuzu_query_result result;
    kuzu_connection_query(conn, "MATCH (p:person) RETURN p.*", &result);

    uint64_t num_tuples = kuzu_query_result_get_num_tuples(&result);
    kuzu_value** tuples = (kuzu_value**)malloc(sizeof(kuzu_value*) * num_tuples);
    for (uint64_t i = 0; i < num_tuples; ++i) {
        kuzu_flat_tuple tuple;
        kuzu_query_result_get_next(&result, &tuple);
        tuples[i] = copy_flat_tuple(&tuple, kuzu_query_result_get_num_columns(&result));
        kuzu_flat_tuple_destroy(&tuple);
    }

    for (uint64_t i = 0; i < num_tuples; ++i) {
        for (uint64_t j = 0; j < kuzu_query_result_get_num_columns(&result); ++j) {
            ASSERT_FALSE(kuzu_value_is_null(&tuples[i][j]));
            kuzu_value_destroy(&tuples[i][j]);
        }
        free(tuples[i]);
    }

    free((void*)tuples);

    kuzu_query_result_destroy(&result);
}

TEST_F(CApiQueryResultTest, GetErrorMessage) {
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection, "MATCH (a:person) RETURN COUNT(*)", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    char* errorMessage = kuzu_query_result_get_error_message(&result);
    kuzu_query_result_destroy(&result);

    state = kuzu_connection_query(connection, "MATCH (a:personnnn) RETURN COUNT(*)", &result);
    ASSERT_EQ(state, KuzuError);
    ASSERT_FALSE(kuzu_query_result_is_success(&result));
    errorMessage = kuzu_query_result_get_error_message(&result);
    ASSERT_EQ(std::string(errorMessage), "Binder exception: Table personnnn does not exist.");
    kuzu_query_result_destroy(&result);
    kuzu_destroy_string(errorMessage);
}

TEST_F(CApiQueryResultTest, ToString) {
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection, "MATCH (a:person) RETURN COUNT(*)", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    char* str_repr = kuzu_query_result_to_string(&result);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_destroy_string(str_repr);
    kuzu_query_result_destroy(&result);
}

TEST_F(CApiQueryResultTest, GetNumColumns) {
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection, "MATCH (a:person) RETURN a.fName, a.age, a.height",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_EQ(kuzu_query_result_get_num_columns(&result), 3);
    kuzu_query_result_destroy(&result);
}

TEST_F(CApiQueryResultTest, GetColumnName) {
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection, "MATCH (a:person) RETURN a.fName, a.age, a.height",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    char* columnName;
    ASSERT_EQ(kuzu_query_result_get_column_name(&result, 0, &columnName), KuzuSuccess);
    ASSERT_EQ(std::string(columnName), "a.fName");
    kuzu_destroy_string(columnName);
    ASSERT_EQ(kuzu_query_result_get_column_name(&result, 1, &columnName), KuzuSuccess);
    ASSERT_EQ(std::string(columnName), "a.age");
    kuzu_destroy_string(columnName);
    ASSERT_EQ(kuzu_query_result_get_column_name(&result, 2, &columnName), KuzuSuccess);
    ASSERT_EQ(std::string(columnName), "a.height");
    kuzu_destroy_string(columnName);
    ASSERT_EQ(kuzu_query_result_get_column_name(&result, 222, &columnName), KuzuError);
    kuzu_query_result_destroy(&result);
}

TEST_F(CApiQueryResultTest, GetColumnDataType) {
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection, "MATCH (a:person) RETURN a.fName, a.age, a.height",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    kuzu_logical_type type;
    ASSERT_EQ(kuzu_query_result_get_column_data_type(&result, 0, &type), KuzuSuccess);
    auto typeCpp = (LogicalType*)(type._data_type);
    ASSERT_EQ(typeCpp->getLogicalTypeID(), LogicalTypeID::STRING);
    kuzu_data_type_destroy(&type);
    ASSERT_EQ(kuzu_query_result_get_column_data_type(&result, 1, &type), KuzuSuccess);
    typeCpp = (LogicalType*)(type._data_type);
    ASSERT_EQ(typeCpp->getLogicalTypeID(), LogicalTypeID::INT64);
    kuzu_data_type_destroy(&type);
    ASSERT_EQ(kuzu_query_result_get_column_data_type(&result, 2, &type), KuzuSuccess);
    typeCpp = (LogicalType*)(type._data_type);
    ASSERT_EQ(typeCpp->getLogicalTypeID(), LogicalTypeID::FLOAT);
    kuzu_data_type_destroy(&type);
    ASSERT_EQ(kuzu_query_result_get_column_data_type(&result, 222, &type), KuzuError);
    kuzu_query_result_destroy(&result);
}

// TODO(Guodong): Fix this test by adding support of STRUCT in arrow table export.
// TEST_F(CApiQueryResultTest, GetArrowSchema) {
//    auto connection = getConnection();
//    auto result = kuzu_connection_query(
//        connection, "MATCH (p:person)-[k:knows]-(q:person) RETURN p.fName, k, q.fName");
//    ASSERT_TRUE(kuzu_query_result_is_success(result));
//    auto schema = kuzu_query_result_get_arrow_schema(result);
//    ASSERT_STREQ(schema.name, "kuzu_query_result");
//    ASSERT_EQ(schema.n_children, 3);
//    ASSERT_STREQ(schema.children[0]->name, "p.fName");
//    ASSERT_STREQ(schema.children[1]->name, "k");
//    ASSERT_STREQ(schema.children[2]->name, "q.fName");
//
//    schema.release(&schema);
//    kuzu_query_result_destroy(result);
//}

TEST_F(CApiQueryResultTest, GetQuerySummary) {
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection, "MATCH (a:person) RETURN a.fName, a.age, a.height",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    kuzu_query_summary summary;
    state = kuzu_query_result_get_query_summary(&result, &summary);
    ASSERT_EQ(state, KuzuSuccess);
    auto compilingTime = kuzu_query_summary_get_compiling_time(&summary);
    ASSERT_GT(compilingTime, 0);
    auto executionTime = kuzu_query_summary_get_execution_time(&summary);
    ASSERT_GT(executionTime, 0);
    kuzu_query_summary_destroy(&summary);
    kuzu_query_result_destroy(&result);
}

TEST_F(CApiQueryResultTest, GetNext) {
    kuzu_query_result result;
    kuzu_flat_tuple row;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        "MATCH (a:person) RETURN a.fName, a.age ORDER BY a.fName", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));

    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &row);
    ASSERT_EQ(state, KuzuSuccess);
    auto flatTupleCpp = (FlatTuple*)(row._flat_tuple);
    ASSERT_EQ(flatTupleCpp->getValue(0)->getValue<std::string>(), "Alice");
    ASSERT_EQ(flatTupleCpp->getValue(1)->getValue<int64_t>(), 35);

    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &row);
    ASSERT_EQ(state, KuzuSuccess);
    flatTupleCpp = (FlatTuple*)(row._flat_tuple);
    ASSERT_EQ(flatTupleCpp->getValue(0)->getValue<std::string>(), "Bob");
    ASSERT_EQ(flatTupleCpp->getValue(1)->getValue<int64_t>(), 30);
    kuzu_flat_tuple_destroy(&row);

    while (kuzu_query_result_has_next(&result)) {
        kuzu_query_result_get_next(&result, &row);
    }
    ASSERT_FALSE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &row);
    ASSERT_EQ(state, KuzuError);
    kuzu_query_result_destroy(&result);
}

TEST_F(CApiQueryResultTest, ResetIterator) {
    kuzu_query_result result;
    kuzu_flat_tuple row;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        "MATCH (a:person) RETURN a.fName, a.age ORDER BY a.fName", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));

    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &row);
    ASSERT_EQ(state, KuzuSuccess);
    auto flatTupleCpp = (FlatTuple*)(row._flat_tuple);
    ASSERT_EQ(flatTupleCpp->getValue(0)->getValue<std::string>(), "Alice");
    ASSERT_EQ(flatTupleCpp->getValue(1)->getValue<int64_t>(), 35);

    kuzu_query_result_reset_iterator(&result);

    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &row);
    ASSERT_EQ(state, KuzuSuccess);
    flatTupleCpp = (FlatTuple*)(row._flat_tuple);
    ASSERT_EQ(flatTupleCpp->getValue(0)->getValue<std::string>(), "Alice");
    ASSERT_EQ(flatTupleCpp->getValue(1)->getValue<int64_t>(), 35);
    kuzu_flat_tuple_destroy(&row);

    kuzu_query_result_destroy(&result);
}

TEST_F(CApiQueryResultTest, MultipleQuery) {
    kuzu_query_result result;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection, "return 1; return 2; return 3;", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));

    char* str = kuzu_query_result_to_string(&result);
    ASSERT_EQ(std::string(str), "1\n1\n");
    kuzu_destroy_string(str);

    ASSERT_TRUE(kuzu_query_result_has_next_query_result(&result));
    kuzu_query_result next_query_result;
    ASSERT_EQ(kuzu_query_result_get_next_query_result(&result, &next_query_result), KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&next_query_result));
    str = kuzu_query_result_to_string(&next_query_result);
    ASSERT_EQ(std::string(str), "2\n2\n");
    kuzu_destroy_string(str);
    kuzu_query_result_destroy(&next_query_result);

    ASSERT_EQ(kuzu_query_result_get_next_query_result(&result, &next_query_result), KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&next_query_result));
    str = kuzu_query_result_to_string(&next_query_result);
    ASSERT_EQ(std::string(str), "3\n3\n");
    kuzu_destroy_string(str);

    ASSERT_FALSE(kuzu_query_result_has_next_query_result(&result));
    ASSERT_EQ(kuzu_query_result_get_next_query_result(&result, &next_query_result), KuzuError);
    kuzu_query_result_destroy(&next_query_result);

    kuzu_query_result_destroy(&result);
}
