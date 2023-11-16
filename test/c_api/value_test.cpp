#include "c_api_test/c_api_test.h"

using namespace kuzu::main;
using namespace kuzu::common;
using namespace kuzu::testing;

class CApiValueTest : public CApiTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

TEST(CApiValueTestEmptyDB, CreateNull) {
    kuzu_value* value = kuzu_value_create_null();
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::ANY);
    ASSERT_EQ(cppValue->isNull(), true);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateNullWithDatatype) {
    auto type = kuzu_data_type_create(kuzu_data_type_id::KUZU_INT64, nullptr, 0);
    kuzu_value* value = kuzu_value_create_null_with_data_type(type);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    kuzu_data_type_destroy(type);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::INT64);
    ASSERT_EQ(cppValue->isNull(), true);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, IsNull) {
    kuzu_value* value = kuzu_value_create_int64(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    kuzu_value_destroy(value);
    value = kuzu_value_create_null();
    ASSERT_TRUE(kuzu_value_is_null(value));
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, SetNull) {
    kuzu_value* value = kuzu_value_create_int64(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    kuzu_value_set_null(value, true);
    ASSERT_TRUE(kuzu_value_is_null(value));
    kuzu_value_set_null(value, false);
    ASSERT_FALSE(kuzu_value_is_null(value));
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateDefault) {
    auto type = kuzu_data_type_create(kuzu_data_type_id::KUZU_INT64, nullptr, 0);
    kuzu_value* value = kuzu_value_create_default(type);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    kuzu_data_type_destroy(type);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::INT64);
    ASSERT_EQ(cppValue->getValue<int64_t>(), 0);
    kuzu_value_destroy(value);

    type = kuzu_data_type_create(kuzu_data_type_id::KUZU_STRING, nullptr, 0);
    value = kuzu_value_create_default(type);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    kuzu_data_type_destroy(type);
    cppValue = static_cast<Value*>(value->_value);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(cppValue->getValue<std::string>(), "");
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateBool) {
    kuzu_value* value = kuzu_value_create_bool(true);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::BOOL);
    ASSERT_EQ(cppValue->getValue<bool>(), true);
    kuzu_value_destroy(value);

    value = kuzu_value_create_bool(false);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::BOOL);
    ASSERT_EQ(cppValue->getValue<bool>(), false);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateInt8) {
    kuzu_value* value = kuzu_value_create_int8(12);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::INT8);
    ASSERT_EQ(cppValue->getValue<int8_t>(), 12);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateInt16) {
    kuzu_value* value = kuzu_value_create_int16(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::INT16);
    ASSERT_EQ(cppValue->getValue<int16_t>(), 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateInt32) {
    kuzu_value* value = kuzu_value_create_int32(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::INT32);
    ASSERT_EQ(cppValue->getValue<int32_t>(), 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateInt64) {
    kuzu_value* value = kuzu_value_create_int64(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::INT64);
    ASSERT_EQ(cppValue->getValue<int64_t>(), 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateUInt8) {
    kuzu_value* value = kuzu_value_create_uint8(12);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::UINT8);
    ASSERT_EQ(cppValue->getValue<uint8_t>(), 12);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateUInt16) {
    kuzu_value* value = kuzu_value_create_uint16(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::UINT16);
    ASSERT_EQ(cppValue->getValue<uint16_t>(), 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateUInt32) {
    kuzu_value* value = kuzu_value_create_uint32(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::UINT32);
    ASSERT_EQ(cppValue->getValue<uint32_t>(), 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateUInt64) {
    kuzu_value* value = kuzu_value_create_uint64(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::UINT64);
    ASSERT_EQ(cppValue->getValue<uint64_t>(), 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateINT128) {
    kuzu_value* value = kuzu_value_create_int128(kuzu_int128_t{211111111, 100000000});
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::INT128);
    auto cppTimeStamp = cppValue->getValue<int128_t>();
    ASSERT_EQ(cppTimeStamp.high, 100000000);
    ASSERT_EQ(cppTimeStamp.low, 211111111);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateFloat) {
    kuzu_value* value = kuzu_value_create_float(123.456);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::FLOAT);
    ASSERT_FLOAT_EQ(cppValue->getValue<float>(), 123.456);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateDouble) {
    kuzu_value* value = kuzu_value_create_double(123.456);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::DOUBLE);
    ASSERT_DOUBLE_EQ(cppValue->getValue<double>(), 123.456);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateInternalID) {
    auto internalID = kuzu_internal_id_t{1, 123};
    kuzu_value* value = kuzu_value_create_internal_id(internalID);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::INTERNAL_ID);
    auto internalIDCpp = cppValue->getValue<internalID_t>();
    ASSERT_EQ(internalIDCpp.tableID, 1);
    ASSERT_EQ(internalIDCpp.offset, 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateDate) {
    kuzu_value* value = kuzu_value_create_date(kuzu_date_t{123});
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::DATE);
    auto cppDate = cppValue->getValue<date_t>();
    ASSERT_EQ(cppDate.days, 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateTimeStamp) {
    kuzu_value* value = kuzu_value_create_timestamp(kuzu_timestamp_t{123});
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::TIMESTAMP);
    auto cppTimeStamp = cppValue->getValue<timestamp_t>();
    ASSERT_EQ(cppTimeStamp.value, 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateInterval) {
    kuzu_value* value = kuzu_value_create_interval(kuzu_interval_t{12, 3, 300});
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::INTERVAL);
    auto cppTimeStamp = cppValue->getValue<interval_t>();
    ASSERT_EQ(cppTimeStamp.months, 12);
    ASSERT_EQ(cppTimeStamp.days, 3);
    ASSERT_EQ(cppTimeStamp.micros, 300);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateString) {
    kuzu_value* value = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(cppValue->getValue<std::string>(), "abcdefg");
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, Clone) {
    kuzu_value* value = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(cppValue->getValue<std::string>(), "abcdefg");

    kuzu_value* clone = kuzu_value_clone(value);
    kuzu_value_destroy(value);

    ASSERT_FALSE(clone->_is_owned_by_cpp);
    auto cppClone = static_cast<Value*>(clone->_value);
    ASSERT_EQ(cppClone->getDataType()->getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(cppClone->getValue<std::string>(), "abcdefg");
    kuzu_value_destroy(clone);
}

TEST(CApiValueTestEmptyDB, Copy) {
    kuzu_value* value = kuzu_value_create_string((char*)"abc");

    kuzu_value* value2 = kuzu_value_create_string((char*)"abcdefg");
    kuzu_value_copy(value, value2);
    kuzu_value_destroy(value2);

    ASSERT_FALSE(kuzu_value_is_null(value));
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType()->getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(cppValue->getValue<std::string>(), "abcdefg");
    kuzu_value_destroy(value);
}

TEST_F(CApiValueTest, GetListSize) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(
        connection, (char*)"MATCH (a:person) RETURN a.workedHours ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(kuzu_value_get_list_size(value), 2);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetListElement) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(
        connection, (char*)"MATCH (a:person) RETURN a.workedHours ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(kuzu_value_get_list_size(value), 2);

    auto listElement = kuzu_value_get_list_element(value, 0);
    ASSERT_TRUE(listElement->_is_owned_by_cpp);
    ASSERT_EQ(kuzu_value_get_int64(listElement), 10);
    kuzu_value_destroy(listElement);

    listElement = kuzu_value_get_list_element(value, 1);
    ASSERT_TRUE(listElement->_is_owned_by_cpp);
    ASSERT_EQ(kuzu_value_get_int64(listElement), 5);
    kuzu_value_destroy(listElement);

    listElement = kuzu_value_get_list_element(value, 222);
    ASSERT_EQ(listElement, nullptr);

    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetStructNumFields) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(
        connection, (char*)"MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_EQ(kuzu_value_get_struct_num_fields(value), 10);

    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetStructFieldName) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(
        connection, (char*)"MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    auto fieldName = kuzu_value_get_struct_field_name(value, 0);
    ASSERT_STREQ(fieldName, "rating");
    free(fieldName);

    fieldName = kuzu_value_get_struct_field_name(value, 1);
    ASSERT_STREQ(fieldName, "stars");
    free(fieldName);

    fieldName = kuzu_value_get_struct_field_name(value, 2);
    ASSERT_STREQ(fieldName, "views");
    free(fieldName);

    fieldName = kuzu_value_get_struct_field_name(value, 3);
    ASSERT_STREQ(fieldName, "release");
    free(fieldName);

    fieldName = kuzu_value_get_struct_field_name(value, 4);
    ASSERT_STREQ(fieldName, "film");
    free(fieldName);

    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetStructFieldValue) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(
        connection, (char*)"MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);

    auto fieldValue = kuzu_value_get_struct_field_value(value, 0);
    auto fieldType = kuzu_value_get_data_type(fieldValue);
    ASSERT_EQ(kuzu_data_type_get_id(fieldType), KUZU_DOUBLE);
    ASSERT_DOUBLE_EQ(kuzu_value_get_double(fieldValue), 1223);
    kuzu_data_type_destroy(fieldType);
    kuzu_value_destroy(fieldValue);

    fieldValue = kuzu_value_get_struct_field_value(value, 1);
    fieldType = kuzu_value_get_data_type(fieldValue);
    kuzu_data_type_destroy(fieldType);
    kuzu_value_destroy(fieldValue);

    fieldValue = kuzu_value_get_struct_field_value(value, 2);
    fieldType = kuzu_value_get_data_type(fieldValue);
    ASSERT_EQ(kuzu_data_type_get_id(fieldType), KUZU_INT64);
    ASSERT_EQ(kuzu_value_get_int64(fieldValue), 10003);
    kuzu_data_type_destroy(fieldType);
    kuzu_value_destroy(fieldValue);

    fieldValue = kuzu_value_get_struct_field_value(value, 3);
    fieldType = kuzu_value_get_data_type(fieldValue);
    ASSERT_EQ(kuzu_data_type_get_id(fieldType), KUZU_TIMESTAMP);
    auto timestamp = kuzu_value_get_timestamp(fieldValue);
    ASSERT_EQ(timestamp.value, 1297442662000000);
    kuzu_data_type_destroy(fieldType);
    kuzu_value_destroy(fieldValue);

    fieldValue = kuzu_value_get_struct_field_value(value, 4);
    fieldType = kuzu_value_get_data_type(fieldValue);
    ASSERT_EQ(kuzu_data_type_get_id(fieldType), KUZU_DATE);
    auto date = kuzu_value_get_date(fieldValue);
    ASSERT_EQ(date.days, 15758);
    kuzu_data_type_destroy(fieldType);
    kuzu_value_destroy(fieldValue);

    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetDataType) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(
        connection, (char*)"MATCH (a:person) RETURN a.fName, a.isStudent, a.workedHours");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    auto dataType = kuzu_value_get_data_type(value);
    ASSERT_EQ(kuzu_data_type_get_id(dataType), KUZU_STRING);
    kuzu_data_type_destroy(dataType);
    kuzu_value_destroy(value);

    value = kuzu_flat_tuple_get_value(flatTuple, 1);
    dataType = kuzu_value_get_data_type(value);
    ASSERT_EQ(kuzu_data_type_get_id(dataType), KUZU_BOOL);
    kuzu_data_type_destroy(dataType);
    kuzu_value_destroy(value);

    value = kuzu_flat_tuple_get_value(flatTuple, 2);
    dataType = kuzu_value_get_data_type(value);
    ASSERT_EQ(kuzu_data_type_get_id(dataType), KUZU_VAR_LIST);
    kuzu_data_type_destroy(dataType);
    kuzu_value_destroy(value);

    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetBool) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(
        connection, (char*)"MATCH (a:person) RETURN a.isStudent ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(kuzu_value_get_bool(value), true);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetInt8) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.level ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(kuzu_value_get_int8(value), 5);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetInt16) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.length ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(kuzu_value_get_int16(value), 5);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetInt32) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(
        connection, (char*)"MATCH (m:movies) RETURN m.length ORDER BY m.name");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(kuzu_value_get_int32(value), 298);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetInt64) {
    auto connection = getConnection();
    auto result =
        kuzu_connection_query(connection, (char*)"MATCH (a:person) RETURN a.ID ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(kuzu_value_get_int64(value), 0);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetUInt8) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.ulevel ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(kuzu_value_get_uint8(value), 250);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetUInt16) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.ulength ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(kuzu_value_get_uint16(value), 33768);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetUInt32) {
    auto connection = getConnection();
    auto result =
        kuzu_connection_query(connection, (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) "
                                                 "RETURN r.temprature ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(kuzu_value_get_uint32(value), 32800);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetUInt64) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.code ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(kuzu_value_get_uint64(value), 9223372036854775808ull);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetInt128) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.hugedata ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    auto int128 = kuzu_value_get_int128(value);
    ASSERT_EQ(int128.high, 100000000);
    ASSERT_EQ(int128.low, 211111111);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, StringToInt128Test) {
    char input[] = "1844674407370955161811111111";
    auto int128_val = kuzu_int128_t_from_string(input);
    ASSERT_EQ(int128_val.high, 100000000);
    ASSERT_EQ(int128_val.low, 211111111);
}

TEST_F(CApiValueTest, Int128ToStringTest) {
    auto int128_val = kuzu_int128_t{211111111, 100000000};
    char* str = kuzu_int128_t_to_string(int128_val);
    ASSERT_STREQ(str, "1844674407370955161811111111");
    free(str);
}

TEST_F(CApiValueTest, GetFloat) {
    auto connection = getConnection();
    auto result =
        kuzu_connection_query(connection, (char*)"MATCH (a:person) RETURN a.height ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_FLOAT_EQ(kuzu_value_get_float(value), 1.731);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetDouble) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(
        connection, (char*)"MATCH (a:person) RETURN a.eyeSight ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_DOUBLE_EQ(kuzu_value_get_double(value), 5.0);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetInternalID) {
    auto connection = getConnection();
    auto result =
        kuzu_connection_query(connection, (char*)"MATCH (a:person) RETURN a ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    auto nodeIDVal = kuzu_value_get_struct_field_value(value, 0 /* internal ID field idx */);
    auto internalID = kuzu_value_get_internal_id(nodeIDVal);
    ASSERT_EQ(internalID.table_id, 0);
    ASSERT_EQ(internalID.offset, 0);
    kuzu_value_destroy(nodeIDVal);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetRelVal) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(
        connection, (char*)"MATCH (a:person) -[r:knows]-> (b:person) RETURN r ORDER BY a.ID, b.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto rel = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(rel->_is_owned_by_cpp);
    auto relSrcIDVal = kuzu_rel_val_get_src_id_val(rel);
    auto relSrcID = kuzu_value_get_internal_id(relSrcIDVal);
    ASSERT_EQ(relSrcID.table_id, 0);
    ASSERT_EQ(relSrcID.offset, 0);
    auto relDstIDVal = kuzu_rel_val_get_dst_id_val(rel);
    auto relDstID = kuzu_value_get_internal_id(relDstIDVal);
    ASSERT_EQ(relDstID.table_id, 0);
    ASSERT_EQ(relDstID.offset, 1);
    auto relLabel = kuzu_rel_val_get_label_val(rel);
    auto relLabelStr = kuzu_value_get_string(relLabel);
    ASSERT_STREQ(relLabelStr, "knows");
    auto propertiesSize = kuzu_rel_val_get_property_size(rel);
    ASSERT_EQ(propertiesSize, 6);
    free(relLabelStr);
    kuzu_value_destroy(relLabel);
    kuzu_value_destroy(relSrcIDVal);
    kuzu_value_destroy(relDstIDVal);
    kuzu_value_destroy(rel);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetDate) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(
        connection, (char*)"MATCH (a:person) RETURN a.birthdate ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    auto date = kuzu_value_get_date(value);
    ASSERT_EQ(date.days, -25567);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetTimestamp) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(
        connection, (char*)"MATCH (a:person) RETURN a.registerTime ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    auto timestamp = kuzu_value_get_timestamp(value);
    ASSERT_EQ(timestamp.value, 1313839530000000);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetInterval) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(
        connection, (char*)"MATCH (a:person) RETURN a.lastJobDuration ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    auto interval = kuzu_value_get_interval(value);
    ASSERT_EQ(interval.months, 36);
    ASSERT_EQ(interval.days, 2);
    ASSERT_EQ(interval.micros, 46920000000);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetString) {
    auto connection = getConnection();
    auto result =
        kuzu_connection_query(connection, (char*)"MATCH (a:person) RETURN a.fName ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    auto str = kuzu_value_get_string(value);
    ASSERT_STREQ(str, "Alice");
    free(str);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, GetBlob) {
    auto connection = getConnection();
    auto result =
        kuzu_connection_query(connection, (char*)R"(RETURN BLOB('\\xAA\\xBB\\xCD\\x1A');)");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    auto blob = kuzu_value_get_blob(value);
    ASSERT_EQ(blob[0], 0xAA);
    ASSERT_EQ(blob[1], 0xBB);
    ASSERT_EQ(blob[2], 0xCD);
    ASSERT_EQ(blob[3], 0x1A);
    ASSERT_EQ(blob[4], 0x00);
    free(blob);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, ToSting) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.fName, a.isStudent, a.workedHours ORDER BY "
               "a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));

    auto flatTuple = kuzu_query_result_get_next(result);

    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    auto str = kuzu_value_to_string(value);
    ASSERT_STREQ(str, "Alice");
    free(str);
    kuzu_value_destroy(value);

    value = kuzu_flat_tuple_get_value(flatTuple, 1);
    str = kuzu_value_to_string(value);
    ASSERT_STREQ(str, "True");
    free(str);
    kuzu_value_destroy(value);

    value = kuzu_flat_tuple_get_value(flatTuple, 2);
    str = kuzu_value_to_string(value);
    ASSERT_STREQ(str, "[10,5]");
    free(str);
    kuzu_value_destroy(value);

    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, NodeValGetLabelVal) {
    auto connection = getConnection();
    auto result =
        kuzu_connection_query(connection, (char*)"MATCH (a:person) RETURN a ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));

    auto flatTuple = kuzu_query_result_get_next(result);
    auto nodeVal = kuzu_flat_tuple_get_value(flatTuple, 0);
    auto labelVal = kuzu_node_val_get_label_val(nodeVal);
    auto labelStr = kuzu_value_get_string(labelVal);
    ASSERT_STREQ(labelStr, "person");
    free(labelStr);
    kuzu_value_destroy(labelVal);
    kuzu_value_destroy(nodeVal);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, NodeValGetID) {
    auto connection = getConnection();
    auto result =
        kuzu_connection_query(connection, (char*)"MATCH (a:person) RETURN a ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));

    auto flatTuple = kuzu_query_result_get_next(result);
    auto nodeVal = kuzu_flat_tuple_get_value(flatTuple, 0);
    auto nodeIDVal = kuzu_node_val_get_id_val(nodeVal);
    ASSERT_NE(nodeIDVal, nullptr);
    auto internalID = kuzu_value_get_internal_id(nodeIDVal);
    ASSERT_EQ(internalID.table_id, 0);
    ASSERT_EQ(internalID.offset, 0);
    kuzu_value_destroy(nodeIDVal);
    kuzu_value_destroy(nodeVal);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, NodeValGetLabelName) {
    auto connection = getConnection();
    auto result =
        kuzu_connection_query(connection, (char*)"MATCH (a:person) RETURN a ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));

    auto flatTuple = kuzu_query_result_get_next(result);
    auto nodeVal = kuzu_flat_tuple_get_value(flatTuple, 0);
    auto labelVal = kuzu_node_val_get_label_val(nodeVal);
    auto labelStr = kuzu_value_get_string(labelVal);
    ASSERT_STREQ(labelStr, "person");
    free(labelStr);
    kuzu_value_destroy(labelVal);
    kuzu_value_destroy(nodeVal);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, NodeValGetProperty) {
    auto connection = getConnection();
    auto result =
        kuzu_connection_query(connection, (char*)"MATCH (a:person) RETURN a ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto node = kuzu_flat_tuple_get_value(flatTuple, 0);
    auto propertyName = kuzu_node_val_get_property_name_at(node, 0);
    ASSERT_STREQ(propertyName, "ID");
    free(propertyName);
    propertyName = kuzu_node_val_get_property_name_at(node, 1);
    ASSERT_STREQ(propertyName, "fName");
    free(propertyName);
    propertyName = kuzu_node_val_get_property_name_at(node, 2);
    ASSERT_STREQ(propertyName, "gender");
    free(propertyName);
    propertyName = kuzu_node_val_get_property_name_at(node, 3);
    ASSERT_STREQ(propertyName, "isStudent");
    free(propertyName);

    auto propertyValue = kuzu_node_val_get_property_value_at(node, 0);
    auto propertyValueID = kuzu_value_get_int64(propertyValue);
    ASSERT_EQ(propertyValueID, 0);
    kuzu_value_destroy(propertyValue);
    propertyValue = kuzu_node_val_get_property_value_at(node, 1);
    auto propertyValuefName = kuzu_value_get_string(propertyValue);
    ASSERT_STREQ(propertyValuefName, "Alice");
    free(propertyValuefName);
    kuzu_value_destroy(propertyValue);
    propertyValue = kuzu_node_val_get_property_value_at(node, 2);
    auto propertyValueGender = kuzu_value_get_int64(propertyValue);
    ASSERT_EQ(propertyValueGender, 1);
    kuzu_value_destroy(propertyValue);
    propertyValue = kuzu_node_val_get_property_value_at(node, 3);
    auto propertyValueIsStudent = kuzu_value_get_bool(propertyValue);
    ASSERT_EQ(propertyValueIsStudent, true);
    kuzu_value_destroy(propertyValue);

    kuzu_value_destroy(node);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, NodeValToString) {
    auto connection = getConnection();
    auto result =
        kuzu_connection_query(connection, (char*)"MATCH (b:organisation) RETURN b ORDER BY b.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto node = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(node->_is_owned_by_cpp);

    auto str = kuzu_node_val_to_string(node);
    ASSERT_STREQ(str,
        "{_ID: 1:0, _LABEL: organisation, ID: 1, name: ABFsUni, orgCode: 325, mark: 3.700000, "
        "score: -2, history: 10 years 5 months 13 hours 24 us, licenseValidInterval: 3 years "
        "5 days, rating: 1.000000, state: {revenue: 138, location: ['toronto', 'montr,eal'], "
        "stock: {price: [96,56], volume: 1000}}, info: 3.120000}");
    free(str);

    kuzu_value_destroy(node);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, RelValGetProperty) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[e:workAt]-> (b:organisation) RETURN e ORDER BY a.ID, b.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto rel = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(rel->_is_owned_by_cpp);
    auto propertiesSize = kuzu_rel_val_get_property_size(rel);
    ASSERT_EQ(propertiesSize, 3);

    auto propertyName = kuzu_rel_val_get_property_name_at(rel, 0);
    ASSERT_STREQ(propertyName, "year");
    free(propertyName);
    propertyName = kuzu_rel_val_get_property_name_at(rel, 1);
    ASSERT_STREQ(propertyName, "grading");
    free(propertyName);
    propertyName = kuzu_rel_val_get_property_name_at(rel, 2);
    ASSERT_STREQ(propertyName, "rating");
    free(propertyName);

    auto propertyValue = kuzu_rel_val_get_property_value_at(rel, 0);
    auto propertyValueYear = kuzu_value_get_int64(propertyValue);
    ASSERT_EQ(propertyValueYear, 2015);
    kuzu_value_destroy(propertyValue);

    propertyValue = kuzu_rel_val_get_property_value_at(rel, 1);
    auto listValue = kuzu_value_get_list_element(propertyValue, 0);
    auto listValueGrading = kuzu_value_get_double(listValue);
    ASSERT_DOUBLE_EQ(listValueGrading, 3.8);
    kuzu_value_destroy(listValue);
    listValue = kuzu_value_get_list_element(propertyValue, 1);
    listValueGrading = kuzu_value_get_double(listValue);
    ASSERT_DOUBLE_EQ(listValueGrading, 2.5);
    kuzu_value_destroy(listValue);
    kuzu_value_destroy(propertyValue);

    propertyValue = kuzu_rel_val_get_property_value_at(rel, 2);
    auto propertyValueRating = kuzu_value_get_float(propertyValue);
    ASSERT_FLOAT_EQ(propertyValueRating, 8.2);
    kuzu_value_destroy(propertyValue);

    kuzu_value_destroy(rel);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, RelValToString) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[e:workAt]-> (b:organisation) RETURN e ORDER BY a.ID, b.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto rel = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(rel->_is_owned_by_cpp);
    auto str = kuzu_rel_val_to_string(rel);
    ASSERT_STREQ(str, "(0:2)-{_LABEL: workAt, _ID: 5:0, year: 2015, grading: [3.800000,2.500000], "
                      "rating: 8.200000}->(1:1)");
    free(str);
    kuzu_value_destroy(rel);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}
