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
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::ANY);
    ASSERT_EQ(cppValue->isNull(), true);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateNullWithDatatype) {
    kuzu_logical_type type;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_INT64, nullptr, 0, &type);
    kuzu_value* value = kuzu_value_create_null_with_data_type(&type);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    kuzu_data_type_destroy(&type);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT64);
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
    kuzu_logical_type type;
    kuzu_data_type_create(kuzu_data_type_id::KUZU_INT64, nullptr, 0, &type);
    kuzu_value* value = kuzu_value_create_default(&type);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    kuzu_data_type_destroy(&type);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT64);
    ASSERT_EQ(cppValue->getValue<int64_t>(), 0);
    kuzu_value_destroy(value);

    kuzu_data_type_create(kuzu_data_type_id::KUZU_STRING, nullptr, 0, &type);
    value = kuzu_value_create_default(&type);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    kuzu_data_type_destroy(&type);
    cppValue = static_cast<Value*>(value->_value);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(cppValue->getValue<std::string>(), "");
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateBool) {
    kuzu_value* value = kuzu_value_create_bool(true);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::BOOL);
    ASSERT_EQ(cppValue->getValue<bool>(), true);
    kuzu_value_destroy(value);

    value = kuzu_value_create_bool(false);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::BOOL);
    ASSERT_EQ(cppValue->getValue<bool>(), false);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateInt8) {
    kuzu_value* value = kuzu_value_create_int8(12);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT8);
    ASSERT_EQ(cppValue->getValue<int8_t>(), 12);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateInt16) {
    kuzu_value* value = kuzu_value_create_int16(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT16);
    ASSERT_EQ(cppValue->getValue<int16_t>(), 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateInt32) {
    kuzu_value* value = kuzu_value_create_int32(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT32);
    ASSERT_EQ(cppValue->getValue<int32_t>(), 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateInt64) {
    kuzu_value* value = kuzu_value_create_int64(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT64);
    ASSERT_EQ(cppValue->getValue<int64_t>(), 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateUInt8) {
    kuzu_value* value = kuzu_value_create_uint8(12);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::UINT8);
    ASSERT_EQ(cppValue->getValue<uint8_t>(), 12);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateUInt16) {
    kuzu_value* value = kuzu_value_create_uint16(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::UINT16);
    ASSERT_EQ(cppValue->getValue<uint16_t>(), 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateUInt32) {
    kuzu_value* value = kuzu_value_create_uint32(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::UINT32);
    ASSERT_EQ(cppValue->getValue<uint32_t>(), 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateUInt64) {
    kuzu_value* value = kuzu_value_create_uint64(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::UINT64);
    ASSERT_EQ(cppValue->getValue<uint64_t>(), 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateINT128) {
    kuzu_value* value = kuzu_value_create_int128(kuzu_int128_t{211111111, 100000000});
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT128);
    auto cppTimeStamp = cppValue->getValue<int128_t>();
    ASSERT_EQ(cppTimeStamp.high, 100000000);
    ASSERT_EQ(cppTimeStamp.low, 211111111);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateFloat) {
    kuzu_value* value = kuzu_value_create_float(123.456);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::FLOAT);
    ASSERT_FLOAT_EQ(cppValue->getValue<float>(), 123.456);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateDouble) {
    kuzu_value* value = kuzu_value_create_double(123.456);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::DOUBLE);
    ASSERT_DOUBLE_EQ(cppValue->getValue<double>(), 123.456);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateInternalID) {
    auto internalID = kuzu_internal_id_t{1, 123};
    kuzu_value* value = kuzu_value_create_internal_id(internalID);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INTERNAL_ID);
    auto internalIDCpp = cppValue->getValue<internalID_t>();
    ASSERT_EQ(internalIDCpp.tableID, 1);
    ASSERT_EQ(internalIDCpp.offset, 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateDate) {
    kuzu_value* value = kuzu_value_create_date(kuzu_date_t{123});
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::DATE);
    auto cppDate = cppValue->getValue<date_t>();
    ASSERT_EQ(cppDate.days, 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateTimeStamp) {
    kuzu_value* value = kuzu_value_create_timestamp(kuzu_timestamp_t{123});
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::TIMESTAMP);
    auto cppTimeStamp = cppValue->getValue<timestamp_t>();
    ASSERT_EQ(cppTimeStamp.value, 123);
    kuzu_value_destroy(value);
}

TEST(CApiValueTestEmptyDB, CreateTimeStampNonStandard) {
    kuzu_value* value_ns = kuzu_value_create_timestamp_ns(kuzu_timestamp_ns_t{12345});
    kuzu_value* value_ms = kuzu_value_create_timestamp_ms(kuzu_timestamp_ms_t{123456});
    kuzu_value* value_sec = kuzu_value_create_timestamp_sec(kuzu_timestamp_sec_t{1234567});
    kuzu_value* value_tz = kuzu_value_create_timestamp_tz(kuzu_timestamp_tz_t{12345678});

    ASSERT_FALSE(value_ns->_is_owned_by_cpp);
    ASSERT_FALSE(value_ms->_is_owned_by_cpp);
    ASSERT_FALSE(value_sec->_is_owned_by_cpp);
    ASSERT_FALSE(value_tz->_is_owned_by_cpp);
    auto cppValue_ns = static_cast<Value*>(value_ns->_value);
    auto cppValue_ms = static_cast<Value*>(value_ms->_value);
    auto cppValue_sec = static_cast<Value*>(value_sec->_value);
    auto cppValue_tz = static_cast<Value*>(value_tz->_value);
    ASSERT_EQ(cppValue_ns->getDataType().getLogicalTypeID(), LogicalTypeID::TIMESTAMP_NS);
    ASSERT_EQ(cppValue_ms->getDataType().getLogicalTypeID(), LogicalTypeID::TIMESTAMP_MS);
    ASSERT_EQ(cppValue_sec->getDataType().getLogicalTypeID(), LogicalTypeID::TIMESTAMP_SEC);
    ASSERT_EQ(cppValue_tz->getDataType().getLogicalTypeID(), LogicalTypeID::TIMESTAMP_TZ);

    auto cppTimeStamp_ns = cppValue_ns->getValue<timestamp_ns_t>();
    auto cppTimeStamp_ms = cppValue_ms->getValue<timestamp_ms_t>();
    auto cppTimeStamp_sec = cppValue_sec->getValue<timestamp_sec_t>();
    auto cppTimeStamp_tz = cppValue_tz->getValue<timestamp_tz_t>();
    ASSERT_EQ(cppTimeStamp_ns.value, 12345);
    ASSERT_EQ(cppTimeStamp_ms.value, 123456);
    ASSERT_EQ(cppTimeStamp_sec.value, 1234567);
    ASSERT_EQ(cppTimeStamp_tz.value, 12345678);
    kuzu_value_destroy(value_ns);
    kuzu_value_destroy(value_ms);
    kuzu_value_destroy(value_sec);
    kuzu_value_destroy(value_tz);
}

TEST(CApiValueTestEmptyDB, CreateInterval) {
    kuzu_value* value = kuzu_value_create_interval(kuzu_interval_t{12, 3, 300});
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INTERVAL);
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
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(cppValue->getValue<std::string>(), "abcdefg");
    kuzu_value_destroy(value);
}

TEST_F(CApiValueTest, CreateList) {
    auto connection = getConnection();
    kuzu_value* value1 = kuzu_value_create_int64(123);
    kuzu_value* value2 = kuzu_value_create_int64(456);
    kuzu_value* value3 = kuzu_value_create_int64(789);
    kuzu_value* value4 = kuzu_value_create_int64(101112);
    kuzu_value* value5 = kuzu_value_create_int64(131415);
    kuzu_value* elements[] = {value1, value2, value3, value4, value5};
    kuzu_value* value = nullptr;
    kuzu_state state = kuzu_value_create_list(5, elements, &value);
    ASSERT_EQ(state, KuzuSuccess);
    // Destroy the original values, the list should still be valid
    for (int i = 0; i < 5; ++i) {
        kuzu_value_destroy(elements[i]);
    }
    ASSERT_FALSE(value->_is_owned_by_cpp);
    kuzu_prepared_statement stmt;
    state = kuzu_connection_prepare(connection, (char*)"RETURN $1", &stmt);
    ASSERT_EQ(state, KuzuSuccess);
    state = kuzu_prepared_statement_bind_value(&stmt, "1", value);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_query_result result;
    state = kuzu_connection_execute(connection, &stmt, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    kuzu_flat_tuple flatTuple;
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value outValue;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &outValue), KuzuSuccess);
    ASSERT_TRUE(outValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&outValue));
    uint64_t size;
    ASSERT_EQ(kuzu_value_get_list_size(&outValue, &size), KuzuSuccess);
    ASSERT_EQ(size, 5);
    kuzu_value listElement;
    ASSERT_EQ(kuzu_value_get_list_element(&outValue, 0, &listElement), KuzuSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    int64_t int64Result;
    ASSERT_EQ(kuzu_value_get_int64(&listElement, &int64Result), KuzuSuccess);
    ASSERT_EQ(int64Result, 123);
    kuzu_value_destroy(&listElement);
    ASSERT_EQ(kuzu_value_get_list_element(&outValue, 1, &listElement), KuzuSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    ASSERT_EQ(kuzu_value_get_int64(&listElement, &int64Result), KuzuSuccess);
    ASSERT_EQ(int64Result, 456);
    kuzu_value_destroy(&listElement);
    ASSERT_EQ(kuzu_value_get_list_element(&outValue, 2, &listElement), KuzuSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    ASSERT_EQ(kuzu_value_get_int64(&listElement, &int64Result), KuzuSuccess);
    ASSERT_EQ(int64Result, 789);
    kuzu_value_destroy(&listElement);
    ASSERT_EQ(kuzu_value_get_list_element(&outValue, 3, &listElement), KuzuSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    ASSERT_EQ(kuzu_value_get_int64(&listElement, &int64Result), KuzuSuccess);
    ASSERT_EQ(int64Result, 101112);
    kuzu_value_destroy(&listElement);
    ASSERT_EQ(kuzu_value_get_list_element(&outValue, 4, &listElement), KuzuSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    ASSERT_EQ(kuzu_value_get_int64(&listElement, &int64Result), KuzuSuccess);
    ASSERT_EQ(int64Result, 131415);
    kuzu_value_destroy(&listElement);
    kuzu_value_destroy(&outValue);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&stmt);
}

TEST(CApiValueTestEmptyDB, CreateListDifferentTypes) {
    kuzu_value* value1 = kuzu_value_create_int64(123);
    kuzu_value* value2 = kuzu_value_create_string((char*)"abcdefg");
    kuzu_value* elements[] = {value1, value2};
    kuzu_value* value = nullptr;
    kuzu_state state = kuzu_value_create_list(2, elements, &value);
    ASSERT_EQ(state, KuzuError);
    kuzu_value_destroy(value1);
    kuzu_value_destroy(value2);
}

TEST(CApiValueTestEmptyDB, CreateListEmpty) {
    kuzu_value* elements[] = {nullptr}; // Must be non-empty
    kuzu_value* value = nullptr;
    kuzu_state state = kuzu_value_create_list(0, elements, &value);
    ASSERT_EQ(state, KuzuError);
}

TEST_F(CApiValueTest, CreateListNested) {
    auto connection = getConnection();
    kuzu_value* value1 = kuzu_value_create_int64(123);
    kuzu_value* value2 = kuzu_value_create_int64(456);
    kuzu_value* value3 = kuzu_value_create_int64(789);
    kuzu_value* value4 = kuzu_value_create_int64(101112);
    kuzu_value* value5 = kuzu_value_create_int64(131415);
    kuzu_value* elements1[] = {value1, value2, value3};
    kuzu_value* elements2[] = {value4, value5};
    kuzu_value* list1 = nullptr;
    kuzu_value* list2 = nullptr;
    kuzu_value_create_list(3, elements1, &list1);
    ASSERT_FALSE(list1->_is_owned_by_cpp);
    kuzu_value_create_list(2, elements2, &list2);
    ASSERT_FALSE(list2->_is_owned_by_cpp);
    kuzu_value* elements[] = {list1, list2};
    kuzu_value* nestedList = nullptr;
    kuzu_state state = kuzu_value_create_list(2, elements, &nestedList);
    ASSERT_EQ(state, KuzuSuccess);
    // Destroy the original values, the list should still be valid
    for (int i = 0; i < 3; ++i) {
        kuzu_value_destroy(elements1[i]);
    }
    for (int i = 0; i < 2; ++i) {
        kuzu_value_destroy(elements2[i]);
    }
    kuzu_value_destroy(list1);
    kuzu_value_destroy(list2);
    ASSERT_FALSE(nestedList->_is_owned_by_cpp);
    kuzu_prepared_statement stmt;
    state = kuzu_connection_prepare(connection, (char*)"RETURN $1", &stmt);
    ASSERT_EQ(state, KuzuSuccess);
    state = kuzu_prepared_statement_bind_value(&stmt, "1", nestedList);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_query_result result;
    state = kuzu_connection_execute(connection, &stmt, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    kuzu_flat_tuple flatTuple;
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value outValue;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &outValue), KuzuSuccess);
    ASSERT_TRUE(outValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&outValue));
    uint64_t size;
    ASSERT_EQ(kuzu_value_get_list_size(&outValue, &size), KuzuSuccess);
    ASSERT_EQ(size, 2);
    kuzu_value listElement;
    ASSERT_EQ(kuzu_value_get_list_element(&outValue, 0, &listElement), KuzuSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&listElement));
    ASSERT_EQ(kuzu_value_get_list_size(&listElement, &size), KuzuSuccess);
    ASSERT_EQ(size, 3);
    kuzu_value innerListElement;
    ASSERT_EQ(kuzu_value_get_list_element(&listElement, 0, &innerListElement), KuzuSuccess);
    ASSERT_TRUE(innerListElement._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&innerListElement));
    int64_t int64Result;
    ASSERT_EQ(kuzu_value_get_int64(&innerListElement, &int64Result), KuzuSuccess);
    ASSERT_EQ(int64Result, 123);
    kuzu_value_destroy(&innerListElement);
    ASSERT_EQ(kuzu_value_get_list_element(&listElement, 1, &innerListElement), KuzuSuccess);
    ASSERT_TRUE(innerListElement._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&innerListElement));
    ASSERT_EQ(kuzu_value_get_int64(&innerListElement, &int64Result), KuzuSuccess);
    ASSERT_EQ(int64Result, 456);
    kuzu_value_destroy(&innerListElement);
    ASSERT_EQ(kuzu_value_get_list_element(&listElement, 2, &innerListElement), KuzuSuccess);
    ASSERT_TRUE(innerListElement._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&innerListElement));
    ASSERT_EQ(kuzu_value_get_int64(&innerListElement, &int64Result), KuzuSuccess);
    ASSERT_EQ(int64Result, 789);
    kuzu_value_destroy(&innerListElement);
    kuzu_value_destroy(&listElement);
    ASSERT_EQ(kuzu_value_get_list_element(&outValue, 1, &listElement), KuzuSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&listElement));
    ASSERT_EQ(kuzu_value_get_list_size(&listElement, &size), KuzuSuccess);
    ASSERT_EQ(size, 2);
    kuzu_value_destroy(&listElement);
    kuzu_value_destroy(&outValue);
    kuzu_value_destroy(nestedList);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&stmt);
}

TEST_F(CApiValueTest, CreateStruct) {
    auto connection = getConnection();
    kuzu_value* value1 = kuzu_value_create_int16(32);
    kuzu_value* value2 = kuzu_value_create_string((char*)"Wong");
    kuzu_value* value3 = kuzu_value_create_string((char*)"Kelley");
    kuzu_value* value4 = kuzu_value_create_int64(123456);
    kuzu_value* value5 = kuzu_value_create_string((char*)"CEO");
    kuzu_value* value6 = kuzu_value_create_bool(true);
    kuzu_value* employmentElements[] = {value5, value6};
    const char* employmentFieldNames[] = {(char*)"title", (char*)"is_current"};
    kuzu_value* employment = nullptr;
    kuzu_state state =
        kuzu_value_create_struct(2, employmentFieldNames, employmentElements, &employment);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_FALSE(employment->_is_owned_by_cpp);
    kuzu_value_destroy(value5);
    kuzu_value_destroy(value6);
    kuzu_value* personElements[] = {value1, value2, value3, value4, employment};
    const char* personFieldNames[] = {(char*)"age", (char*)"first_name", (char*)"last_name",
        (char*)"id", (char*)"employment"};
    kuzu_value* person = nullptr;
    state = kuzu_value_create_struct(5, personFieldNames, personElements, &person);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value_destroy(value1);
    kuzu_value_destroy(value2);
    kuzu_value_destroy(value3);
    kuzu_value_destroy(value4);
    kuzu_value_destroy(employment);
    ASSERT_FALSE(person->_is_owned_by_cpp);
    kuzu_prepared_statement stmt;
    state = kuzu_connection_prepare(connection, (char*)"RETURN $1", &stmt);
    ASSERT_EQ(state, KuzuSuccess);
    state = kuzu_prepared_statement_bind_value(&stmt, "1", person);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_query_result result;
    state = kuzu_connection_execute(connection, &stmt, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    kuzu_flat_tuple flatTuple;
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value outValue;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &outValue), KuzuSuccess);
    ASSERT_TRUE(outValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&outValue));
    uint64_t size;
    state = kuzu_value_get_struct_num_fields(&outValue, &size);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_EQ(size, 5);
    char* structFieldName;
    kuzu_value structFieldValue;
    state = kuzu_value_get_struct_field_name(&outValue, 0, &structFieldName);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_STREQ(structFieldName, "age");
    state = kuzu_value_get_struct_field_value(&outValue, 0, &structFieldValue);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(structFieldValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&structFieldValue));
    int16_t int16Result;
    state = kuzu_value_get_int16(&structFieldValue, &int16Result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_EQ(int16Result, 32);
    kuzu_value_destroy(&structFieldValue);
    kuzu_destroy_string(structFieldName);
    state = kuzu_value_get_struct_field_name(&outValue, 1, &structFieldName);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_STREQ(structFieldName, "first_name");
    state = kuzu_value_get_struct_field_value(&outValue, 1, &structFieldValue);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(structFieldValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&structFieldValue));
    char* stringResult;
    state = kuzu_value_get_string(&structFieldValue, &stringResult);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_STREQ(stringResult, "Wong");
    kuzu_value_destroy(&structFieldValue);
    kuzu_destroy_string(structFieldName);
    kuzu_destroy_string(stringResult);
    state = kuzu_value_get_struct_field_name(&outValue, 2, &structFieldName);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_STREQ(structFieldName, "last_name");
    state = kuzu_value_get_struct_field_value(&outValue, 2, &structFieldValue);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(structFieldValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&structFieldValue));
    state = kuzu_value_get_string(&structFieldValue, &stringResult);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_STREQ(stringResult, "Kelley");
    kuzu_value_destroy(&structFieldValue);
    kuzu_destroy_string(structFieldName);
    kuzu_destroy_string(stringResult);
    state = kuzu_value_get_struct_field_name(&outValue, 3, &structFieldName);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_STREQ(structFieldName, "id");
    state = kuzu_value_get_struct_field_value(&outValue, 3, &structFieldValue);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(structFieldValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&structFieldValue));
    int64_t int64Result;
    state = kuzu_value_get_int64(&structFieldValue, &int64Result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_EQ(int64Result, 123456);
    kuzu_value_destroy(&structFieldValue);
    kuzu_destroy_string(structFieldName);
    state = kuzu_value_get_struct_field_name(&outValue, 4, &structFieldName);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_STREQ(structFieldName, "employment");
    state = kuzu_value_get_struct_field_value(&outValue, 4, &structFieldValue);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(structFieldValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&structFieldValue));
    state = kuzu_value_get_struct_num_fields(&structFieldValue, &size);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_EQ(size, 2);
    char* employmentFieldName;
    kuzu_value employmentFieldValue;
    state = kuzu_value_get_struct_field_name(&structFieldValue, 0, &employmentFieldName);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_STREQ(employmentFieldName, "title");
    state = kuzu_value_get_struct_field_value(&structFieldValue, 0, &employmentFieldValue);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(employmentFieldValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&employmentFieldValue));
    state = kuzu_value_get_string(&employmentFieldValue, &stringResult);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_STREQ(stringResult, "CEO");
    kuzu_value_destroy(&employmentFieldValue);
    kuzu_destroy_string(employmentFieldName);
    kuzu_destroy_string(stringResult);
    state = kuzu_value_get_struct_field_name(&structFieldValue, 1, &employmentFieldName);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_STREQ(employmentFieldName, "is_current");
    state = kuzu_value_get_struct_field_value(&structFieldValue, 1, &employmentFieldValue);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(employmentFieldValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&employmentFieldValue));
    bool boolResult;
    state = kuzu_value_get_bool(&employmentFieldValue, &boolResult);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_EQ(boolResult, true);
    kuzu_value_destroy(&employmentFieldValue);
    kuzu_destroy_string(employmentFieldName);
    kuzu_value_destroy(&structFieldValue);
    kuzu_destroy_string(structFieldName);
    kuzu_value_destroy(&outValue);
    kuzu_value_destroy(person);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&stmt);
}

TEST(CApiValueTestEmptyDB, CreateStructEmpty) {
    const char* fieldNames[] = {(char*)"name"}; // Must be non-empty
    kuzu_value* values[] = {nullptr};           // Must be non-empty
    kuzu_value* value = nullptr;
    kuzu_state state = kuzu_value_create_struct(0, fieldNames, values, &value);
    ASSERT_EQ(state, KuzuError);
}

TEST_F(CApiValueTest, CreateMap) {
    auto connection = getConnection();
    kuzu_value* key1 = kuzu_value_create_int64(1);
    kuzu_value* value1 = kuzu_value_create_string((char*)"one");
    kuzu_value* key2 = kuzu_value_create_int64(2);
    kuzu_value* value2 = kuzu_value_create_string((char*)"two");
    kuzu_value* key3 = kuzu_value_create_int64(3);
    kuzu_value* value3 = kuzu_value_create_string((char*)"three");
    kuzu_value* keys[] = {key1, key2, key3};
    kuzu_value* values[] = {value1, value2, value3};
    kuzu_value* map = nullptr;
    kuzu_state state = kuzu_value_create_map(3, keys, values, &map);
    ASSERT_EQ(state, KuzuSuccess);
    // Destroy the original values, the map should still be valid
    for (int i = 0; i < 3; ++i) {
        kuzu_value_destroy(keys[i]);
        kuzu_value_destroy(values[i]);
    }
    ASSERT_FALSE(map->_is_owned_by_cpp);
    kuzu_prepared_statement stmt;
    state = kuzu_connection_prepare(connection, (char*)"RETURN $1", &stmt);
    ASSERT_EQ(state, KuzuSuccess);
    state = kuzu_prepared_statement_bind_value(&stmt, "1", map);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_query_result result;
    state = kuzu_connection_execute(connection, &stmt, &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    kuzu_flat_tuple flatTuple;
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value outValue;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &outValue), KuzuSuccess);
    ASSERT_TRUE(outValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&outValue));
    uint64_t size;
    ASSERT_EQ(kuzu_value_get_map_size(&outValue, &size), KuzuSuccess);
    ASSERT_EQ(size, 3);
    kuzu_value mapValue;
    ASSERT_EQ(kuzu_value_get_map_value(&outValue, 0, &mapValue), KuzuSuccess);
    ASSERT_TRUE(mapValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&mapValue));
    char* stringResult;
    ASSERT_EQ(kuzu_value_get_string(&mapValue, &stringResult), KuzuSuccess);
    ASSERT_STREQ(stringResult, "one");
    kuzu_value_destroy(&mapValue);
    kuzu_destroy_string(stringResult);
    ASSERT_EQ(kuzu_value_get_map_value(&outValue, 1, &mapValue), KuzuSuccess);
    ASSERT_TRUE(mapValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&mapValue));
    ASSERT_EQ(kuzu_value_get_string(&mapValue, &stringResult), KuzuSuccess);
    ASSERT_STREQ(stringResult, "two");
    kuzu_value_destroy(&mapValue);
    kuzu_destroy_string(stringResult);
    ASSERT_EQ(kuzu_value_get_map_value(&outValue, 2, &mapValue), KuzuSuccess);
    ASSERT_TRUE(mapValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&mapValue));
    ASSERT_EQ(kuzu_value_get_string(&mapValue, &stringResult), KuzuSuccess);
    ASSERT_STREQ(stringResult, "three");
    kuzu_value_destroy(&mapValue);
    kuzu_destroy_string(stringResult);
    ASSERT_EQ(kuzu_value_get_map_key(&outValue, 0, &mapValue), KuzuSuccess);
    ASSERT_TRUE(mapValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&mapValue));
    int64_t int64Result;
    ASSERT_EQ(kuzu_value_get_int64(&mapValue, &int64Result), KuzuSuccess);
    ASSERT_EQ(int64Result, 1);
    kuzu_value_destroy(&mapValue);
    ASSERT_EQ(kuzu_value_get_map_key(&outValue, 1, &mapValue), KuzuSuccess);
    ASSERT_TRUE(mapValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&mapValue));
    ASSERT_EQ(kuzu_value_get_int64(&mapValue, &int64Result), KuzuSuccess);
    ASSERT_EQ(int64Result, 2);
    kuzu_value_destroy(&mapValue);
    ASSERT_EQ(kuzu_value_get_map_key(&outValue, 2, &mapValue), KuzuSuccess);
    ASSERT_TRUE(mapValue._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&mapValue));
    ASSERT_EQ(kuzu_value_get_int64(&mapValue, &int64Result), KuzuSuccess);
    ASSERT_EQ(int64Result, 3);
    kuzu_value_destroy(&mapValue);
    kuzu_value_destroy(&outValue);
    kuzu_value_destroy(map);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);
    kuzu_prepared_statement_destroy(&stmt);
}

TEST(CApiValueTestEmptyDB, CreateMapEmpty) {
    kuzu_value* keys[] = {nullptr};   // Must be non-empty
    kuzu_value* values[] = {nullptr}; // Must be non-empty
    kuzu_value* map = nullptr;
    kuzu_state state = kuzu_value_create_map(0, keys, values, &map);
    ASSERT_EQ(state, KuzuError);
}

TEST(CApiValueTestEmptyDB, Clone) {
    kuzu_value* value = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(cppValue->getValue<std::string>(), "abcdefg");

    kuzu_value* clone = kuzu_value_clone(value);
    kuzu_value_destroy(value);

    ASSERT_FALSE(clone->_is_owned_by_cpp);
    auto cppClone = static_cast<Value*>(clone->_value);
    ASSERT_EQ(cppClone->getDataType().getLogicalTypeID(), LogicalTypeID::STRING);
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
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(cppValue->getValue<std::string>(), "abcdefg");
    kuzu_value_destroy(value);
}

TEST_F(CApiValueTest, GetListSize) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.workedHours ORDER BY a.ID", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    uint64_t size;
    ASSERT_EQ(kuzu_value_get_list_size(&value, &size), KuzuSuccess);
    ASSERT_EQ(size, 2);

    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_list_size(badValue, &size), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetListElement) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.workedHours ORDER BY a.ID", &result);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    uint64_t size;
    ASSERT_EQ(kuzu_value_get_list_size(&value, &size), KuzuSuccess);
    ASSERT_EQ(size, 2);

    kuzu_value listElement;
    ASSERT_EQ(kuzu_value_get_list_element(&value, 0, &listElement), KuzuSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    int64_t int64Result;
    ASSERT_EQ(kuzu_value_get_int64(&listElement, &int64Result), KuzuSuccess);
    ASSERT_EQ(int64Result, 10);

    ASSERT_EQ(kuzu_value_get_list_element(&value, 1, &listElement), KuzuSuccess);
    ASSERT_TRUE(listElement._is_owned_by_cpp);
    ASSERT_EQ(kuzu_value_get_int64(&listElement, &int64Result), KuzuSuccess);
    ASSERT_EQ(int64Result, 5);
    kuzu_value_destroy(&listElement);

    ASSERT_EQ(kuzu_value_get_list_element(&value, 222, &listElement), KuzuError);

    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);
}

TEST_F(CApiValueTest, GetStructNumFields) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    kuzu_flat_tuple_get_value(&flatTuple, 0, &value);
    uint64_t numFields;
    ASSERT_EQ(kuzu_value_get_struct_num_fields(&value, &numFields), KuzuSuccess);
    ASSERT_EQ(numFields, 14);

    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_struct_num_fields(badValue, &numFields), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetStructFieldName) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    char* fieldName;
    ASSERT_EQ(kuzu_value_get_struct_field_name(&value, 0, &fieldName), KuzuSuccess);
    ASSERT_STREQ(fieldName, "rating");
    kuzu_destroy_string(fieldName);

    ASSERT_EQ(kuzu_value_get_struct_field_name(&value, 1, &fieldName), KuzuSuccess);
    ASSERT_STREQ(fieldName, "stars");
    kuzu_destroy_string(fieldName);

    ASSERT_EQ(kuzu_value_get_struct_field_name(&value, 2, &fieldName), KuzuSuccess);
    ASSERT_STREQ(fieldName, "views");
    kuzu_destroy_string(fieldName);

    ASSERT_EQ(kuzu_value_get_struct_field_name(&value, 3, &fieldName), KuzuSuccess);
    ASSERT_STREQ(fieldName, "release");
    kuzu_destroy_string(fieldName);

    ASSERT_EQ(kuzu_value_get_struct_field_name(&value, 4, &fieldName), KuzuSuccess);
    ASSERT_STREQ(fieldName, "release_ns");
    kuzu_destroy_string(fieldName);

    ASSERT_EQ(kuzu_value_get_struct_field_name(&value, 5, &fieldName), KuzuSuccess);
    ASSERT_STREQ(fieldName, "release_ms");
    kuzu_destroy_string(fieldName);

    ASSERT_EQ(kuzu_value_get_struct_field_name(&value, 6, &fieldName), KuzuSuccess);
    ASSERT_STREQ(fieldName, "release_sec");
    kuzu_destroy_string(fieldName);

    ASSERT_EQ(kuzu_value_get_struct_field_name(&value, 7, &fieldName), KuzuSuccess);
    ASSERT_STREQ(fieldName, "release_tz");
    kuzu_destroy_string(fieldName);

    ASSERT_EQ(kuzu_value_get_struct_field_name(&value, 8, &fieldName), KuzuSuccess);
    ASSERT_STREQ(fieldName, "film");
    kuzu_destroy_string(fieldName);

    ASSERT_EQ(kuzu_value_get_struct_field_name(&value, 222, &fieldName), KuzuError);

    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);
}

TEST_F(CApiValueTest, GetStructFieldValue) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (m:movies) WHERE m.name=\"Roma\" RETURN m.description", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);

    kuzu_value fieldValue;
    ASSERT_EQ(kuzu_value_get_struct_field_value(&value, 0, &fieldValue), KuzuSuccess);
    kuzu_logical_type fieldType;
    kuzu_value_get_data_type(&fieldValue, &fieldType);
    ASSERT_EQ(kuzu_data_type_get_id(&fieldType), KUZU_DOUBLE);
    double doubleValue;
    ASSERT_EQ(kuzu_value_get_double(&fieldValue, &doubleValue), KuzuSuccess);
    ASSERT_DOUBLE_EQ(doubleValue, 1223);
    kuzu_data_type_destroy(&fieldType);

    ASSERT_EQ(kuzu_value_get_struct_field_value(&value, 1, &fieldValue), KuzuSuccess);
    kuzu_value_get_data_type(&fieldValue, &fieldType);
    kuzu_data_type_destroy(&fieldType);

    ASSERT_EQ(kuzu_value_get_struct_field_value(&value, 2, &fieldValue), KuzuSuccess);
    kuzu_value_get_data_type(&fieldValue, &fieldType);
    ASSERT_EQ(kuzu_data_type_get_id(&fieldType), KUZU_INT64);
    int64_t int64Value;
    ASSERT_EQ(kuzu_value_get_int64(&fieldValue, &int64Value), KuzuSuccess);
    kuzu_data_type_destroy(&fieldType);

    ASSERT_EQ(kuzu_value_get_struct_field_value(&value, 3, &fieldValue), KuzuSuccess);
    kuzu_value_get_data_type(&fieldValue, &fieldType);
    ASSERT_EQ(kuzu_data_type_get_id(&fieldType), KUZU_TIMESTAMP);
    kuzu_timestamp_t timestamp;
    ASSERT_EQ(kuzu_value_get_timestamp(&fieldValue, &timestamp), KuzuSuccess);
    ASSERT_EQ(timestamp.value, 1297442662000000);
    kuzu_data_type_destroy(&fieldType);

    ASSERT_EQ(kuzu_value_get_struct_field_value(&value, 4, &fieldValue), KuzuSuccess);
    kuzu_value_get_data_type(&fieldValue, &fieldType);
    ASSERT_EQ(kuzu_data_type_get_id(&fieldType), KUZU_TIMESTAMP_NS);
    kuzu_timestamp_ns_t timestamp_ns;
    ASSERT_EQ(kuzu_value_get_timestamp_ns(&fieldValue, &timestamp_ns), KuzuSuccess);
    ASSERT_EQ(timestamp_ns.value, 1297442662123456000);
    kuzu_data_type_destroy(&fieldType);

    ASSERT_EQ(kuzu_value_get_struct_field_value(&value, 5, &fieldValue), KuzuSuccess);
    kuzu_value_get_data_type(&fieldValue, &fieldType);
    ASSERT_EQ(kuzu_data_type_get_id(&fieldType), KUZU_TIMESTAMP_MS);
    kuzu_timestamp_ms_t timestamp_ms;
    ASSERT_EQ(kuzu_value_get_timestamp_ms(&fieldValue, &timestamp_ms), KuzuSuccess);
    ASSERT_EQ(timestamp_ms.value, 1297442662123);
    kuzu_data_type_destroy(&fieldType);

    ASSERT_EQ(kuzu_value_get_struct_field_value(&value, 6, &fieldValue), KuzuSuccess);
    kuzu_value_get_data_type(&fieldValue, &fieldType);
    ASSERT_EQ(kuzu_data_type_get_id(&fieldType), KUZU_TIMESTAMP_SEC);
    kuzu_timestamp_sec_t timestamp_sec;
    ASSERT_EQ(kuzu_value_get_timestamp_sec(&fieldValue, &timestamp_sec), KuzuSuccess);
    ASSERT_EQ(timestamp_sec.value, 1297442662);
    kuzu_data_type_destroy(&fieldType);

    ASSERT_EQ(kuzu_value_get_struct_field_value(&value, 7, &fieldValue), KuzuSuccess);
    kuzu_value_get_data_type(&fieldValue, &fieldType);
    ASSERT_EQ(kuzu_data_type_get_id(&fieldType), KUZU_TIMESTAMP_TZ);
    kuzu_timestamp_tz_t timestamp_tz;
    ASSERT_EQ(kuzu_value_get_timestamp_tz(&fieldValue, &timestamp_tz), KuzuSuccess);
    ASSERT_EQ(timestamp_tz.value, 1297442662123456);
    kuzu_data_type_destroy(&fieldType);

    ASSERT_EQ(kuzu_value_get_struct_field_value(&value, 8, &fieldValue), KuzuSuccess);
    kuzu_value_get_data_type(&fieldValue, &fieldType);
    ASSERT_EQ(kuzu_data_type_get_id(&fieldType), KUZU_DATE);
    kuzu_date_t date;
    ASSERT_EQ(kuzu_value_get_date(&fieldValue, &date), KuzuSuccess);
    ASSERT_EQ(date.days, 15758);
    kuzu_data_type_destroy(&fieldType);
    kuzu_value_destroy(&fieldValue);

    ASSERT_EQ(kuzu_value_get_struct_field_value(&value, 222, &fieldValue), KuzuError);

    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);
}

TEST_F(CApiValueTest, getMapNumFields) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (m:movies) WHERE m.length = 2544 RETURN m.audience", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_FALSE(kuzu_query_result_has_next(&result));
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);

    uint64_t mapFields;
    ASSERT_EQ(kuzu_value_get_map_size(&value, &mapFields), KuzuSuccess);
    ASSERT_EQ(mapFields, 1);

    kuzu_query_result_destroy(&result);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
}

TEST_F(CApiValueTest, getMapKey) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (m:movies) WHERE m.length = 2544 RETURN m.audience", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_FALSE(kuzu_query_result_has_next(&result));
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);

    kuzu_value key;
    ASSERT_EQ(kuzu_value_get_map_key(&value, 0, &key), KuzuSuccess);
    kuzu_logical_type keyType;
    kuzu_value_get_data_type(&key, &keyType);
    ASSERT_EQ(kuzu_data_type_get_id(&keyType), KUZU_STRING);
    char* mapName;
    ASSERT_EQ(kuzu_value_get_string(&key, &mapName), KuzuSuccess);
    ASSERT_STREQ(mapName, "audience1");
    kuzu_destroy_string(mapName);
    kuzu_data_type_destroy(&keyType);
    kuzu_value_destroy(&key);

    ASSERT_EQ(kuzu_value_get_map_key(&value, 1, &key), KuzuError);
    kuzu_query_result_destroy(&result);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
}

TEST_F(CApiValueTest, getMapValue) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (m:movies) WHERE m.length = 2544 RETURN m.audience", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_FALSE(kuzu_query_result_has_next(&result));
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);

    kuzu_value mapValue;
    ASSERT_EQ(kuzu_value_get_map_value(&value, 0, &mapValue), KuzuSuccess);
    kuzu_logical_type mapType;
    kuzu_value_get_data_type(&mapValue, &mapType);
    ASSERT_EQ(kuzu_data_type_get_id(&mapType), KUZU_INT64);
    int64_t mapIntValue;
    ASSERT_EQ(kuzu_value_get_int64(&mapValue, &mapIntValue), KuzuSuccess);
    ASSERT_EQ(mapIntValue, 33);

    ASSERT_EQ(kuzu_value_get_map_value(&value, 1, &mapValue), KuzuError);

    kuzu_data_type_destroy(&mapType);
    kuzu_query_result_destroy(&result);
    kuzu_value_destroy(&mapValue);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
}

TEST_F(CApiValueTest, getDecimalAsString) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"UNWIND [1] AS A UNWIND [5.7, 8.3, 8.7, 13.7] AS B WITH cast(CAST(A AS DECIMAL) "
               "* "
               "CAST(B AS DECIMAL) AS DECIMAL(18, 1)) AS PROD RETURN COLLECT(PROD) AS RES",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);

    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);

    kuzu_logical_type dataType;
    kuzu_value_get_data_type(&value, &dataType);
    ASSERT_EQ(kuzu_data_type_get_id(&dataType), KUZU_LIST);
    uint64_t list_size;
    ASSERT_EQ(kuzu_value_get_list_size(&value, &list_size), KuzuSuccess);
    ASSERT_EQ(list_size, 4);
    kuzu_data_type_destroy(&dataType);

    kuzu_value decimal_entry;
    char* decimal_value;
    std::string decimal_string_value;
    ASSERT_EQ(kuzu_value_get_list_element(&value, 0, &decimal_entry), KuzuSuccess);
    kuzu_value_get_data_type(&decimal_entry, &dataType);
    ASSERT_EQ(kuzu_data_type_get_id(&dataType), KUZU_DECIMAL);
    ASSERT_EQ(kuzu_value_get_decimal_as_string(&decimal_entry, &decimal_value), KuzuSuccess);
    decimal_string_value = std::string(decimal_value);
    ASSERT_EQ(decimal_string_value, "5.7");
    kuzu_destroy_string(decimal_value);
    kuzu_data_type_destroy(&dataType);

    ASSERT_EQ(kuzu_value_get_list_element(&value, 1, &decimal_entry), KuzuSuccess);
    kuzu_value_get_data_type(&decimal_entry, &dataType);
    ASSERT_EQ(kuzu_data_type_get_id(&dataType), KUZU_DECIMAL);
    ASSERT_EQ(kuzu_value_get_decimal_as_string(&decimal_entry, &decimal_value), KuzuSuccess);
    decimal_string_value = std::string(decimal_value);
    ASSERT_EQ(decimal_string_value, "8.3");
    kuzu_destroy_string(decimal_value);
    kuzu_data_type_destroy(&dataType);

    ASSERT_EQ(kuzu_value_get_list_element(&value, 2, &decimal_entry), KuzuSuccess);
    kuzu_value_get_data_type(&decimal_entry, &dataType);
    ASSERT_EQ(kuzu_data_type_get_id(&dataType), KUZU_DECIMAL);
    ASSERT_EQ(kuzu_value_get_decimal_as_string(&decimal_entry, &decimal_value), KuzuSuccess);
    decimal_string_value = std::string(decimal_value);
    ASSERT_EQ(decimal_string_value, "8.7");
    kuzu_destroy_string(decimal_value);
    kuzu_data_type_destroy(&dataType);

    ASSERT_EQ(kuzu_value_get_list_element(&value, 3, &decimal_entry), KuzuSuccess);
    kuzu_value_get_data_type(&decimal_entry, &dataType);
    ASSERT_EQ(kuzu_data_type_get_id(&dataType), KUZU_DECIMAL);
    ASSERT_EQ(kuzu_value_get_decimal_as_string(&decimal_entry, &decimal_value), KuzuSuccess);
    decimal_string_value = std::string(decimal_value);
    ASSERT_EQ(decimal_string_value, "13.7");
    kuzu_destroy_string(decimal_value);
    kuzu_data_type_destroy(&dataType);

    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);
    kuzu_value_destroy(&decimal_entry);
}

TEST_F(CApiValueTest, GetDataType) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.fName, a.isStudent, a.workedHours", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    kuzu_logical_type dataType;
    kuzu_value_get_data_type(&value, &dataType);
    ASSERT_EQ(kuzu_data_type_get_id(&dataType), KUZU_STRING);
    kuzu_data_type_destroy(&dataType);

    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 1, &value), KuzuSuccess);
    kuzu_value_get_data_type(&value, &dataType);
    ASSERT_EQ(kuzu_data_type_get_id(&dataType), KUZU_BOOL);
    kuzu_data_type_destroy(&dataType);

    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 2, &value), KuzuSuccess);
    kuzu_value_get_data_type(&value, &dataType);
    ASSERT_EQ(kuzu_data_type_get_id(&dataType), KUZU_LIST);
    kuzu_data_type_destroy(&dataType);
    kuzu_value_destroy(&value);

    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);
}

TEST_F(CApiValueTest, GetBool) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.isStudent ORDER BY a.ID", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    bool boolValue;
    ASSERT_EQ(kuzu_value_get_bool(&value, &boolValue), KuzuSuccess);
    ASSERT_TRUE(boolValue);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_bool(badValue, &boolValue), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetInt8) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.level ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    int8_t int8Value;
    ASSERT_EQ(kuzu_value_get_int8(&value, &int8Value), KuzuSuccess);
    ASSERT_EQ(int8Value, 5);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_int8(badValue, &int8Value), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetInt16) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.length ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    int16_t int16Value;
    ASSERT_EQ(kuzu_value_get_int16(&value, &int16Value), KuzuSuccess);
    ASSERT_EQ(int16Value, 5);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_int16(badValue, &int16Value), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetInt32) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (m:movies) RETURN m.length ORDER BY m.name", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    int32_t int32Value;
    ASSERT_EQ(kuzu_value_get_int32(&value, &int32Value), KuzuSuccess);
    ASSERT_EQ(int32Value, 298);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_int32(badValue, &int32Value), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetInt64) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection, (char*)"MATCH (a:person) RETURN a.ID ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    int64_t int64Value;
    ASSERT_EQ(kuzu_value_get_int64(&value, &int64Value), KuzuSuccess);
    ASSERT_EQ(int64Value, 0);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_int64(badValue, &int64Value), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetUInt8) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.ulevel ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    uint8_t uint8Value;
    ASSERT_EQ(kuzu_value_get_uint8(&value, &uint8Value), KuzuSuccess);
    ASSERT_EQ(uint8Value, 250);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_uint8(badValue, &uint8Value), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetUInt16) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.ulength ORDER BY "
               "a.ID",
        &result);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    uint16_t uint16Value;
    ASSERT_EQ(kuzu_value_get_uint16(&value, &uint16Value), KuzuSuccess);
    ASSERT_EQ(uint16Value, 33768);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_uint16(badValue, &uint16Value), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetUInt32) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) "
               "RETURN r.temperature ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    uint32_t uint32Value;
    ASSERT_EQ(kuzu_value_get_uint32(&value, &uint32Value), KuzuSuccess);
    ASSERT_EQ(uint32Value, 32800);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_uint32(badValue, &uint32Value), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetUInt64) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.code ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    uint64_t uint64Value;
    ASSERT_EQ(kuzu_value_get_uint64(&value, &uint64Value), KuzuSuccess);
    ASSERT_EQ(uint64Value, 9223372036854775808ull);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_uint64(badValue, &uint64Value), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetInt128) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[r:studyAt]-> (b:organisation) RETURN r.hugedata ORDER BY "
               "a.ID",
        &result);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    kuzu_int128_t int128;
    ASSERT_EQ(kuzu_value_get_int128(&value, &int128), KuzuSuccess);
    ASSERT_EQ(int128.high, 100000000);
    ASSERT_EQ(int128.low, 211111111);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_int128(badValue, &int128), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, StringToInt128Test) {
    char input[] = "1844674407370955161811111111";
    kuzu_int128_t int128_val;
    ASSERT_EQ(kuzu_int128_t_from_string(input, &int128_val), KuzuSuccess);
    ASSERT_EQ(int128_val.high, 100000000);
    ASSERT_EQ(int128_val.low, 211111111);

    char badInput[] = "this is not a int128";
    kuzu_int128_t int128_val2;
    ASSERT_EQ(kuzu_int128_t_from_string(badInput, &int128_val2), KuzuError);
}

TEST_F(CApiValueTest, Int128ToStringTest) {
    auto int128_val = kuzu_int128_t{211111111, 100000000};
    char* str;
    ASSERT_EQ(kuzu_int128_t_to_string(int128_val, &str), KuzuSuccess);
    ASSERT_STREQ(str, "1844674407370955161811111111");
    kuzu_destroy_string(str);
}

TEST_F(CApiValueTest, GetFloat) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.height ORDER BY a.ID", &result);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    float floatValue;
    ASSERT_EQ(kuzu_value_get_float(&value, &floatValue), KuzuSuccess);
    ASSERT_FLOAT_EQ(floatValue, 1.731);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_float(badValue, &floatValue), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetDouble) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.eyeSight ORDER BY a.ID", &result);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    double doubleValue;
    ASSERT_EQ(kuzu_value_get_double(&value, &doubleValue), KuzuSuccess);
    ASSERT_DOUBLE_EQ(doubleValue, 5.0);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_double(badValue, &doubleValue), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetInternalID) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection, (char*)"MATCH (a:person) RETURN a ORDER BY a.ID",
        &result);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    kuzu_value nodeIDVal;
    ASSERT_EQ(kuzu_value_get_struct_field_value(&value, 0 /* internal ID field idx */, &nodeIDVal),
        KuzuSuccess);
    kuzu_internal_id_t internalID;
    ASSERT_EQ(kuzu_value_get_internal_id(&nodeIDVal, &internalID), KuzuSuccess);
    ASSERT_EQ(internalID.table_id, 0);
    ASSERT_EQ(internalID.offset, 0);
    kuzu_value_destroy(&nodeIDVal);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_internal_id(badValue, &internalID), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetRelVal) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[r:knows]-> (b:person) RETURN r ORDER BY a.ID, b.ID", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value rel;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &rel), KuzuSuccess);
    ASSERT_TRUE(rel._is_owned_by_cpp);
    kuzu_value relSrcIDVal;
    ASSERT_EQ(kuzu_rel_val_get_src_id_val(&rel, &relSrcIDVal), KuzuSuccess);
    kuzu_internal_id_t relSrcID;
    ASSERT_EQ(kuzu_value_get_internal_id(&relSrcIDVal, &relSrcID), KuzuSuccess);
    ASSERT_EQ(relSrcID.table_id, 0);
    ASSERT_EQ(relSrcID.offset, 0);
    kuzu_value relDstIDVal;
    ASSERT_EQ(kuzu_rel_val_get_dst_id_val(&rel, &relDstIDVal), KuzuSuccess);
    kuzu_internal_id_t relDstID;
    ASSERT_EQ(kuzu_value_get_internal_id(&relDstIDVal, &relDstID), KuzuSuccess);
    ASSERT_EQ(relDstID.table_id, 0);
    ASSERT_EQ(relDstID.offset, 1);
    kuzu_value relLabel;
    ASSERT_EQ(kuzu_rel_val_get_label_val(&rel, &relLabel), KuzuSuccess);
    char* relLabelStr;
    ASSERT_EQ(kuzu_value_get_string(&relLabel, &relLabelStr), KuzuSuccess);
    ASSERT_STREQ(relLabelStr, "knows");
    uint64_t propertiesSize;
    ASSERT_EQ(kuzu_rel_val_get_property_size(&rel, &propertiesSize), KuzuSuccess);
    ASSERT_EQ(propertiesSize, 7);
    kuzu_destroy_string(relLabelStr);
    kuzu_value_destroy(&relLabel);
    kuzu_value_destroy(&relSrcIDVal);
    kuzu_value_destroy(&relDstIDVal);
    kuzu_value_destroy(&rel);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_rel_val_get_src_id_val(badValue, &relSrcIDVal), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetDate) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.birthdate ORDER BY a.ID", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    kuzu_date_t date;
    ASSERT_EQ(kuzu_value_get_date(&value, &date), KuzuSuccess);
    ASSERT_EQ(date.days, -25567);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_date(badValue, &date), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetTimestamp) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.registerTime ORDER BY a.ID", &result);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    kuzu_timestamp_t timestamp;
    ASSERT_EQ(kuzu_value_get_timestamp(&value, &timestamp), KuzuSuccess);
    ASSERT_EQ(timestamp.value, 1313839530000000);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_timestamp(badValue, &timestamp), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetInterval) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.lastJobDuration ORDER BY a.ID", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    kuzu_interval_t interval;
    ASSERT_EQ(kuzu_value_get_interval(&value, &interval), KuzuSuccess);
    ASSERT_EQ(interval.months, 36);
    ASSERT_EQ(interval.days, 2);
    ASSERT_EQ(interval.micros, 46920000000);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_interval(badValue, &interval), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetString) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.fName ORDER BY a.ID", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    char* str;
    ASSERT_EQ(kuzu_value_get_string(&value, &str), KuzuSuccess);
    ASSERT_STREQ(str, "Alice");
    kuzu_destroy_string(str);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_int32(123);
    ASSERT_EQ(kuzu_value_get_string(badValue, &str), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetBlob) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection, (char*)R"(RETURN BLOB('\\xAA\\xBB\\xCD\\x1A');)",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    uint8_t* blob;
    ASSERT_EQ(kuzu_value_get_blob(&value, &blob), KuzuSuccess);
    ASSERT_EQ(blob[0], 0xAA);
    ASSERT_EQ(blob[1], 0xBB);
    ASSERT_EQ(blob[2], 0xCD);
    ASSERT_EQ(blob[3], 0x1A);
    ASSERT_EQ(blob[4], 0x00);
    kuzu_destroy_blob(blob);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_blob(badValue, &blob), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetUUID) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)R"(RETURN UUID("A0EEBC99-9C0B-4EF8-BB6D-6BB9BD380A11");)", &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value value;
    kuzu_flat_tuple_get_value(&flatTuple, 0, &value);
    ASSERT_TRUE(value._is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(&value));
    char* str;
    ASSERT_EQ(kuzu_value_get_uuid(&value, &str), KuzuSuccess);
    ASSERT_STREQ(str, "a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    kuzu_destroy_string(str);
    kuzu_value_destroy(&value);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_value_get_uuid(badValue, &str), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, ToSting) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) RETURN a.fName, a.isStudent, a.workedHours ORDER BY "
               "a.ID",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));

    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);

    kuzu_value value;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &value), KuzuSuccess);
    char* str = kuzu_value_to_string(&value);
    ASSERT_STREQ(str, "Alice");
    kuzu_destroy_string(str);

    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 1, &value), KuzuSuccess);
    str = kuzu_value_to_string(&value);
    ASSERT_STREQ(str, "True");
    kuzu_destroy_string(str);

    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 2, &value), KuzuSuccess);
    str = kuzu_value_to_string(&value);
    ASSERT_STREQ(str, "[10,5]");
    kuzu_destroy_string(str);
    kuzu_value_destroy(&value);

    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);
}

TEST_F(CApiValueTest, NodeValGetLabelVal) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection, (char*)"MATCH (a:person) RETURN a ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));

    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value nodeVal;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &nodeVal), KuzuSuccess);
    kuzu_value labelVal;
    ASSERT_EQ(kuzu_node_val_get_label_val(&nodeVal, &labelVal), KuzuSuccess);
    char* labelStr;
    ASSERT_EQ(kuzu_value_get_string(&labelVal, &labelStr), KuzuSuccess);
    ASSERT_STREQ(labelStr, "person");
    kuzu_destroy_string(labelStr);
    kuzu_value_destroy(&labelVal);
    kuzu_value_destroy(&nodeVal);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_node_val_get_label_val(badValue, &labelVal), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, NodeValGetID) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection, (char*)"MATCH (a:person) RETURN a ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));

    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value nodeVal;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &nodeVal), KuzuSuccess);
    kuzu_value nodeIDVal;
    ASSERT_EQ(kuzu_node_val_get_id_val(&nodeVal, &nodeIDVal), KuzuSuccess);
    ASSERT_NE(nodeIDVal._value, nullptr);
    kuzu_internal_id_t internalID;
    ASSERT_EQ(kuzu_value_get_internal_id(&nodeIDVal, &internalID), KuzuSuccess);
    ASSERT_EQ(internalID.table_id, 0);
    ASSERT_EQ(internalID.offset, 0);
    kuzu_value_destroy(&nodeIDVal);
    kuzu_value_destroy(&nodeVal);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_node_val_get_id_val(badValue, &nodeIDVal), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, NodeValGetLabelName) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection, (char*)"MATCH (a:person) RETURN a ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));

    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value nodeVal;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &nodeVal), KuzuSuccess);
    kuzu_value labelVal;
    ASSERT_EQ(kuzu_node_val_get_label_val(&nodeVal, &labelVal), KuzuSuccess);
    char* labelStr;
    ASSERT_EQ(kuzu_value_get_string(&labelVal, &labelStr), KuzuSuccess);
    ASSERT_STREQ(labelStr, "person");
    kuzu_destroy_string(labelStr);
    kuzu_value_destroy(&labelVal);
    kuzu_value_destroy(&nodeVal);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_node_val_get_label_val(badValue, &labelVal), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, NodeValGetProperty) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection, (char*)"MATCH (a:person) RETURN a ORDER BY a.ID",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value node;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &node), KuzuSuccess);
    char* propertyName;
    ASSERT_EQ(kuzu_node_val_get_property_name_at(&node, 0, &propertyName), KuzuSuccess);
    ASSERT_STREQ(propertyName, "ID");
    kuzu_destroy_string(propertyName);
    ASSERT_EQ(kuzu_node_val_get_property_name_at(&node, 1, &propertyName), KuzuSuccess);
    ASSERT_STREQ(propertyName, "fName");
    kuzu_destroy_string(propertyName);
    ASSERT_EQ(kuzu_node_val_get_property_name_at(&node, 2, &propertyName), KuzuSuccess);
    ASSERT_STREQ(propertyName, "gender");
    kuzu_destroy_string(propertyName);
    ASSERT_EQ(kuzu_node_val_get_property_name_at(&node, 3, &propertyName), KuzuSuccess);
    ASSERT_STREQ(propertyName, "isStudent");
    kuzu_destroy_string(propertyName);

    kuzu_value propertyValue;
    ASSERT_EQ(kuzu_node_val_get_property_value_at(&node, 0, &propertyValue), KuzuSuccess);
    int64_t propertyValueID;
    ASSERT_EQ(kuzu_value_get_int64(&propertyValue, &propertyValueID), KuzuSuccess);
    ASSERT_EQ(propertyValueID, 0);
    ASSERT_EQ(kuzu_node_val_get_property_value_at(&node, 1, &propertyValue), KuzuSuccess);
    char* propertyValuefName;
    ASSERT_EQ(kuzu_value_get_string(&propertyValue, &propertyValuefName), KuzuSuccess);
    ASSERT_STREQ(propertyValuefName, "Alice");
    kuzu_destroy_string(propertyValuefName);
    ASSERT_EQ(kuzu_node_val_get_property_value_at(&node, 2, &propertyValue), KuzuSuccess);
    int64_t propertyValueGender;
    ASSERT_EQ(kuzu_value_get_int64(&propertyValue, &propertyValueGender), KuzuSuccess);
    ASSERT_EQ(propertyValueGender, 1);
    ASSERT_EQ(kuzu_node_val_get_property_value_at(&node, 3, &propertyValue), KuzuSuccess);
    bool propertyValueIsStudent;
    ASSERT_EQ(kuzu_value_get_bool(&propertyValue, &propertyValueIsStudent), KuzuSuccess);
    ASSERT_EQ(propertyValueIsStudent, true);
    kuzu_value_destroy(&propertyValue);

    kuzu_value_destroy(&node);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_node_val_get_property_name_at(badValue, 0, &propertyName), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, NodeValToString) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (b:organisation) RETURN b ORDER BY b.ID", &result);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value node;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &node), KuzuSuccess);
    ASSERT_TRUE(node._is_owned_by_cpp);

    char* str = kuzu_value_to_string(&node);
    ASSERT_STREQ(str,
        "{_ID: 1:0, _LABEL: organisation, ID: 1, name: ABFsUni, orgCode: 325, mark: 3.700000, "
        "score: -2, history: 10 years 5 months 13 hours 24 us, licenseValidInterval: 3 years "
        "5 days, rating: 1.000000, state: {revenue: 138, location: ['toronto','montr,eal'], "
        "stock: {price: [96,56], volume: 1000}}, info: 3.120000}");
    kuzu_destroy_string(str);

    kuzu_value_destroy(&node);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);
}

TEST_F(CApiValueTest, RelValGetProperty) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[e:workAt]-> (b:organisation) RETURN e ORDER BY a.ID, b.ID",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value rel;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &rel), KuzuSuccess);
    ASSERT_TRUE(rel._is_owned_by_cpp);
    uint64_t propertiesSize;
    ASSERT_EQ(kuzu_rel_val_get_property_size(&rel, &propertiesSize), KuzuSuccess);
    ASSERT_EQ(propertiesSize, 3);

    char* propertyName;
    ASSERT_EQ(kuzu_rel_val_get_property_name_at(&rel, 0, &propertyName), KuzuSuccess);
    ASSERT_STREQ(propertyName, "year");
    kuzu_destroy_string(propertyName);

    ASSERT_EQ(kuzu_rel_val_get_property_name_at(&rel, 1, &propertyName), KuzuSuccess);
    ASSERT_STREQ(propertyName, "grading");
    kuzu_destroy_string(propertyName);
    ASSERT_EQ(kuzu_rel_val_get_property_name_at(&rel, 2, &propertyName), KuzuSuccess);
    ASSERT_STREQ(propertyName, "rating");
    kuzu_destroy_string(propertyName);

    kuzu_value propertyValue;
    ASSERT_EQ(kuzu_rel_val_get_property_value_at(&rel, 0, &propertyValue), KuzuSuccess);
    int64_t propertyValueYear;
    ASSERT_EQ(kuzu_value_get_int64(&propertyValue, &propertyValueYear), KuzuSuccess);
    ASSERT_EQ(propertyValueYear, 2015);

    ASSERT_EQ(kuzu_rel_val_get_property_value_at(&rel, 1, &propertyValue), KuzuSuccess);
    kuzu_value listValue;
    ASSERT_EQ(kuzu_value_get_list_element(&propertyValue, 0, &listValue), KuzuSuccess);
    double listValueGrading;
    ASSERT_EQ(kuzu_value_get_double(&listValue, &listValueGrading), KuzuSuccess);
    ASSERT_DOUBLE_EQ(listValueGrading, 3.8);
    ASSERT_EQ(kuzu_value_get_list_element(&propertyValue, 1, &listValue), KuzuSuccess);
    ASSERT_EQ(kuzu_value_get_double(&listValue, &listValueGrading), KuzuSuccess);
    ASSERT_DOUBLE_EQ(listValueGrading, 2.5);
    kuzu_value_destroy(&listValue);

    ASSERT_EQ(kuzu_rel_val_get_property_value_at(&rel, 2, &propertyValue), KuzuSuccess);
    float propertyValueRating;
    ASSERT_EQ(kuzu_value_get_float(&propertyValue, &propertyValueRating), KuzuSuccess);
    ASSERT_FLOAT_EQ(propertyValueRating, 8.2);
    kuzu_value_destroy(&propertyValue);

    kuzu_value_destroy(&rel);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_rel_val_get_property_name_at(badValue, 0, &propertyName), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, RelValToString) {
    kuzu_query_result result;
    kuzu_flat_tuple flatTuple;
    kuzu_state state;
    auto connection = getConnection();
    state = kuzu_connection_query(connection,
        (char*)"MATCH (a:person) -[e:workAt]-> (b:organisation) RETURN e ORDER BY a.ID, b.ID",
        &result);
    ASSERT_EQ(state, KuzuSuccess);
    ASSERT_TRUE(kuzu_query_result_is_success(&result));
    ASSERT_TRUE(kuzu_query_result_has_next(&result));
    state = kuzu_query_result_get_next(&result, &flatTuple);
    ASSERT_EQ(state, KuzuSuccess);
    kuzu_value rel;
    ASSERT_EQ(kuzu_flat_tuple_get_value(&flatTuple, 0, &rel), KuzuSuccess);
    ASSERT_TRUE(rel._is_owned_by_cpp);
    char* str;
    ASSERT_EQ(kuzu_rel_val_to_string(&rel, &str), KuzuSuccess);
    ASSERT_STREQ(str, "(0:2)-{_LABEL: workAt, _ID: 5:0, year: 2015, grading: [3.800000,2.500000], "
                      "rating: 8.200000}->(1:1)");
    kuzu_destroy_string(str);
    kuzu_value_destroy(&rel);
    kuzu_flat_tuple_destroy(&flatTuple);
    kuzu_query_result_destroy(&result);

    kuzu_value* badValue = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_EQ(kuzu_rel_val_to_string(badValue, &str), KuzuError);
    kuzu_value_destroy(badValue);
}

TEST_F(CApiValueTest, GetTmFromNonStandardTimestamp) {
    kuzu_timestamp_ns_t timestamp_ns = kuzu_timestamp_ns_t{17515323532900000};
    kuzu_timestamp_ms_t timestamp_ms = kuzu_timestamp_ms_t{1012323435341};
    kuzu_timestamp_sec_t timestamp_sec = kuzu_timestamp_sec_t{1432135648};
    kuzu_timestamp_tz_t timestamp_tz = kuzu_timestamp_tz_t{771513532900000};
    struct tm tm;
    ASSERT_EQ(kuzu_timestamp_ns_to_tm(timestamp_ns, &tm), KuzuSuccess);
    ASSERT_EQ(tm.tm_year, 70);
    ASSERT_EQ(tm.tm_mon, 6);
    ASSERT_EQ(tm.tm_mday, 22);
    ASSERT_EQ(tm.tm_hour, 17);
    ASSERT_EQ(tm.tm_min, 22);
    ASSERT_EQ(tm.tm_sec, 3);
    ASSERT_EQ(kuzu_timestamp_ms_to_tm(timestamp_ms, &tm), KuzuSuccess);
    ASSERT_EQ(tm.tm_year, 102);
    ASSERT_EQ(tm.tm_mon, 0);
    ASSERT_EQ(tm.tm_mday, 29);
    ASSERT_EQ(tm.tm_hour, 16);
    ASSERT_EQ(tm.tm_min, 57);
    ASSERT_EQ(tm.tm_sec, 15);
    ASSERT_EQ(kuzu_timestamp_sec_to_tm(timestamp_sec, &tm), KuzuSuccess);
    ASSERT_EQ(tm.tm_year, 115);
    ASSERT_EQ(tm.tm_mon, 4);
    ASSERT_EQ(tm.tm_mday, 20);
    ASSERT_EQ(tm.tm_hour, 15);
    ASSERT_EQ(tm.tm_min, 27);
    ASSERT_EQ(tm.tm_sec, 28);
    ASSERT_EQ(kuzu_timestamp_tz_to_tm(timestamp_tz, &tm), KuzuSuccess);
    ASSERT_EQ(tm.tm_year, 94);
    ASSERT_EQ(tm.tm_mon, 5);
    ASSERT_EQ(tm.tm_mday, 13);
    ASSERT_EQ(tm.tm_hour, 13);
    ASSERT_EQ(tm.tm_min, 18);
    ASSERT_EQ(tm.tm_sec, 52);
}

TEST_F(CApiValueTest, GetTmFromTimestamp) {
    kuzu_timestamp_t timestamp = kuzu_timestamp_t{171513532900000};
    struct tm tm;
    ASSERT_EQ(kuzu_timestamp_to_tm(timestamp, &tm), KuzuSuccess);
    ASSERT_EQ(tm.tm_year, 75);
    ASSERT_EQ(tm.tm_mon, 5);
    ASSERT_EQ(tm.tm_mday, 9);
    ASSERT_EQ(tm.tm_hour, 2);
    ASSERT_EQ(tm.tm_min, 38);
    ASSERT_EQ(tm.tm_sec, 52);
}

TEST_F(CApiValueTest, GetTmFromDate) {
    kuzu_date_t date = kuzu_date_t{-255};
    struct tm tm;
    ASSERT_EQ(kuzu_date_to_tm(date, &tm), KuzuSuccess);
    ASSERT_EQ(tm.tm_year, 69);
    ASSERT_EQ(tm.tm_mon, 3);
    ASSERT_EQ(tm.tm_mday, 21);
    ASSERT_EQ(tm.tm_hour, 0);
    ASSERT_EQ(tm.tm_min, 0);
    ASSERT_EQ(tm.tm_sec, 0);
}

TEST_F(CApiValueTest, GetTimestampFromTm) {
    struct tm tm;
    tm.tm_year = 75;
    tm.tm_mon = 5;
    tm.tm_mday = 9;
    tm.tm_hour = 2;
    tm.tm_min = 38;
    tm.tm_sec = 52;
    kuzu_timestamp_t timestamp;
    ASSERT_EQ(kuzu_timestamp_from_tm(tm, &timestamp), KuzuSuccess);
    ASSERT_EQ(timestamp.value, 171513532000000);
}

TEST_F(CApiValueTest, GetNonStandardTimestampFromTm) {
    struct tm tm;
    tm.tm_year = 70;
    tm.tm_mon = 6;
    tm.tm_mday = 22;
    tm.tm_hour = 17;
    tm.tm_min = 22;
    tm.tm_sec = 3;
    kuzu_timestamp_ns_t timestamp_ns;
    ASSERT_EQ(kuzu_timestamp_ns_from_tm(tm, &timestamp_ns), KuzuSuccess);
    ASSERT_EQ(timestamp_ns.value, 17515323000000000);
    tm.tm_year = 102;
    tm.tm_mon = 0;
    tm.tm_mday = 29;
    tm.tm_hour = 16;
    tm.tm_min = 57;
    tm.tm_sec = 15;
    kuzu_timestamp_ms_t timestamp_ms;
    ASSERT_EQ(kuzu_timestamp_ms_from_tm(tm, &timestamp_ms), KuzuSuccess);
    ASSERT_EQ(timestamp_ms.value, 1012323435000);
    tm.tm_year = 115;
    tm.tm_mon = 4;
    tm.tm_mday = 20;
    tm.tm_hour = 15;
    tm.tm_min = 27;
    tm.tm_sec = 28;
    kuzu_timestamp_sec_t timestamp_sec;
    ASSERT_EQ(kuzu_timestamp_sec_from_tm(tm, &timestamp_sec), KuzuSuccess);
    ASSERT_EQ(timestamp_sec.value, 1432135648);
    tm.tm_year = 94;
    tm.tm_mon = 5;
    tm.tm_mday = 13;
    tm.tm_hour = 13;
    tm.tm_min = 18;
    tm.tm_sec = 52;
    kuzu_timestamp_tz_t timestamp_tz;
    ASSERT_EQ(kuzu_timestamp_tz_from_tm(tm, &timestamp_tz), KuzuSuccess);
    ASSERT_EQ(timestamp_tz.value, 771513532000000);
}

TEST_F(CApiValueTest, GetDateFromTm) {
    struct tm tm;
    tm.tm_year = 69;
    tm.tm_mon = 3;
    tm.tm_mday = 21;
    tm.tm_hour = 0;
    tm.tm_min = 0;
    tm.tm_sec = 0;
    kuzu_date_t date;
    ASSERT_EQ(kuzu_date_from_tm(tm, &date), KuzuSuccess);
    ASSERT_EQ(date.days, -255);
}

TEST_F(CApiValueTest, GetDateFromString) {
    char input[] = "1969-04-21";
    kuzu_date_t date;
    ASSERT_EQ(kuzu_date_from_string(input, &date), KuzuSuccess);
    ASSERT_EQ(date.days, -255);

    char badInput[] = "this is not a date";
    ASSERT_EQ(kuzu_date_from_string(badInput, &date), KuzuError);
}

TEST_F(CApiValueTest, GetStringFromDate) {
    kuzu_date_t date = kuzu_date_t{-255};
    char* str;
    ASSERT_EQ(kuzu_date_to_string(date, &str), KuzuSuccess);
    ASSERT_STREQ(str, "1969-04-21");
    kuzu_destroy_string(str);
}

TEST_F(CApiValueTest, GetDifftimeFromInterval) {
    kuzu_interval_t interval = kuzu_interval_t{36, 2, 46920000000};
    double difftime;
    kuzu_interval_to_difftime(interval, &difftime);
    ASSERT_DOUBLE_EQ(difftime, 93531720);
}

TEST_F(CApiValueTest, GetIntervalFromDifftime) {
    double difftime = 211110160.479;
    kuzu_interval_t interval;
    kuzu_interval_from_difftime(difftime, &interval);
    ASSERT_EQ(interval.months, 81);
    ASSERT_EQ(interval.days, 13);
    ASSERT_EQ(interval.micros, 34960479000);
}
