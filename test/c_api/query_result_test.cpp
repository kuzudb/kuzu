#include "c_api_test/c_api_test.h"

using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::processor;

class CApiQueryResultTest : public CApiTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

TEST_F(CApiQueryResultTest, GetErrorMessage) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(connection, "MATCH (a:person) RETURN COUNT(*)");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    auto errorMessage = kuzu_query_result_get_error_message(result);
    ASSERT_EQ(errorMessage, nullptr);
    kuzu_query_result_destroy(result);

    result = kuzu_connection_query(connection, "MATCH (a:personnnn) RETURN COUNT(*)");
    ASSERT_FALSE(kuzu_query_result_is_success(result));
    errorMessage = kuzu_query_result_get_error_message(result);
    ASSERT_NE(errorMessage, nullptr);
    ASSERT_EQ(std::string(errorMessage), "Binder exception: Node table personnnn does not exist.");
    kuzu_query_result_destroy(result);
    free(errorMessage);
}

TEST_F(CApiQueryResultTest, GetNumColumns) {
    auto connection = getConnection();
    auto result =
        kuzu_connection_query(connection, "MATCH (a:person) RETURN a.fName, a.age, a.height");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_EQ(kuzu_query_result_get_num_columns(result), 3);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiQueryResultTest, GetColumnName) {
    auto connection = getConnection();
    auto result =
        kuzu_connection_query(connection, "MATCH (a:person) RETURN a.fName, a.age, a.height");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    auto columnName = kuzu_query_result_get_column_name(result, 0);
    ASSERT_EQ(std::string(columnName), "a.fName");
    free(columnName);
    columnName = kuzu_query_result_get_column_name(result, 1);
    ASSERT_EQ(std::string(columnName), "a.age");
    free(columnName);
    columnName = kuzu_query_result_get_column_name(result, 2);
    ASSERT_EQ(std::string(columnName), "a.height");
    free(columnName);
    columnName = kuzu_query_result_get_column_name(result, 222);
    ASSERT_EQ(columnName, nullptr);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiQueryResultTest, GetColumnDataType) {
    auto connection = getConnection();
    auto result =
        kuzu_connection_query(connection, "MATCH (a:person) RETURN a.fName, a.age, a.height");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    auto type = kuzu_query_result_get_column_data_type(result, 0);
    auto typeCpp = (LogicalType*)(type->_data_type);
    ASSERT_EQ(typeCpp->getLogicalTypeID(), LogicalTypeID::STRING);
    kuzu_data_type_destroy(type);
    type = kuzu_query_result_get_column_data_type(result, 1);
    typeCpp = (LogicalType*)(type->_data_type);
    ASSERT_EQ(typeCpp->getLogicalTypeID(), LogicalTypeID::INT64);
    kuzu_data_type_destroy(type);
    type = kuzu_query_result_get_column_data_type(result, 2);
    typeCpp = (LogicalType*)(type->_data_type);
    ASSERT_EQ(typeCpp->getLogicalTypeID(), LogicalTypeID::FLOAT);
    kuzu_data_type_destroy(type);
    type = kuzu_query_result_get_column_data_type(result, 222);
    ASSERT_EQ(type, nullptr);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiQueryResultTest, GetQuerySummary) {
    auto connection = getConnection();
    auto result =
        kuzu_connection_query(connection, "MATCH (a:person) RETURN a.fName, a.age, a.height");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    auto summary = kuzu_query_result_get_query_summary(result);
    ASSERT_NE(summary, nullptr);
    auto compilingTime = kuzu_query_summary_get_compiling_time(summary);
    ASSERT_GT(compilingTime, 0);
    auto executionTime = kuzu_query_summary_get_execution_time(summary);
    ASSERT_GT(executionTime, 0);
    kuzu_query_summary_destroy(summary);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiQueryResultTest, GetNext) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(
        connection, "MATCH (a:person) RETURN a.fName, a.age ORDER BY a.fName");
    ASSERT_TRUE(kuzu_query_result_is_success(result));

    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto row = kuzu_query_result_get_next(result);
    auto flatTuplePtrCpp = (std::shared_ptr<FlatTuple>*)(row->_flat_tuple);
    auto flatTupleCpp = *flatTuplePtrCpp;
    ASSERT_EQ(flatTupleCpp->getValue(0)->getValue<std::string>(), "Alice");
    ASSERT_EQ(flatTupleCpp->getValue(1)->getValue<int64_t>(), 35);
    kuzu_flat_tuple_destroy(row);

    ASSERT_TRUE(kuzu_query_result_has_next(result));
    row = kuzu_query_result_get_next(result);
    ASSERT_NE(row, nullptr);
    flatTuplePtrCpp = (std::shared_ptr<FlatTuple>*)(row->_flat_tuple);
    flatTupleCpp = *flatTuplePtrCpp;
    ASSERT_EQ(flatTupleCpp->getValue(0)->getValue<std::string>(), "Bob");
    ASSERT_EQ(flatTupleCpp->getValue(1)->getValue<int64_t>(), 30);
    kuzu_flat_tuple_destroy(row);

    kuzu_query_result_destroy(result);
}

TEST_F(CApiQueryResultTest, WriteToCSV) {
    std::string newline = "\n";
    std::string basicOutput =
        R"(Carol,1,5.000000,1940-06-22,1911-08-20 02:32:21,CsWork)" + newline +
        R"(Dan,2,4.800000,1950-07-23,2031-11-30 12:25:30,DEsWork)" + newline +
        R"(Elizabeth,1,4.700000,1980-10-26,1976-12-23 11:21:42,DEsWork)" + newline;
    auto query = "MATCH (a:person)-[:workAt]->(o:organisation) RETURN a.fName, a.gender,"
                 "a.eyeSight, a.birthdate, a.registerTime, o.name";
    auto connection = getConnection();
    auto result = kuzu_connection_query(connection, query);
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    auto outputPath = databasePath + "/output_CSV_CAPI.csv";
    kuzu_query_result_write_to_csv(result, outputPath.c_str(), ',', '"', '\n');
    std::ifstream f(outputPath);
    std::ostringstream ss;
    ss << f.rdbuf();
    std::string fileString = ss.str();
    ASSERT_STREQ(fileString.c_str(), basicOutput.c_str());
    kuzu_query_result_destroy(result);
}

TEST_F(CApiQueryResultTest, ResetIterator) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(
        connection, "MATCH (a:person) RETURN a.fName, a.age ORDER BY a.fName");
    ASSERT_TRUE(kuzu_query_result_is_success(result));

    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto row = kuzu_query_result_get_next(result);
    auto flatTuplePtrCpp = (std::shared_ptr<FlatTuple>*)(row->_flat_tuple);
    auto flatTupleCpp = *flatTuplePtrCpp;
    ASSERT_EQ(flatTupleCpp->getValue(0)->getValue<std::string>(), "Alice");
    ASSERT_EQ(flatTupleCpp->getValue(1)->getValue<int64_t>(), 35);
    kuzu_flat_tuple_destroy(row);

    kuzu_query_result_reset_iterator(result);

    ASSERT_TRUE(kuzu_query_result_has_next(result));
    row = kuzu_query_result_get_next(result);
    ASSERT_NE(row, nullptr);
    flatTuplePtrCpp = (std::shared_ptr<FlatTuple>*)(row->_flat_tuple);
    flatTupleCpp = *flatTuplePtrCpp;
    ASSERT_EQ(flatTupleCpp->getValue(0)->getValue<std::string>(), "Alice");
    ASSERT_EQ(flatTupleCpp->getValue(1)->getValue<int64_t>(), 35);
    kuzu_flat_tuple_destroy(row);

    kuzu_query_result_destroy(result);
}
