#include "c_api_test/c_api_test.h"
using namespace kuzu::main;
using namespace kuzu::common;

class CApiValueTest : public CApiTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/tinysnb/");
    }
};

TEST_F(CApiValueTest, CreateNull) {
    kuzu_value* value = kuzu_value_create_null();
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::ANY);
    ASSERT_EQ(cppValue->isNull(), true);
    kuzu_value_destroy(value);
}

TEST_F(CApiValueTest, CreateNullWithDatatype) {
    auto type = kuzu_data_type_create(kuzu_data_type_id::KUZU_INT64, nullptr, 0);
    kuzu_value* value = kuzu_value_create_null_with_data_type(type);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    kuzu_data_type_destroy(type);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT64);
    ASSERT_EQ(cppValue->isNull(), true);
    kuzu_value_destroy(value);
}

TEST_F(CApiValueTest, IsNull) {
    kuzu_value* value = kuzu_value_create_int64(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    kuzu_value_destroy(value);
    value = kuzu_value_create_null();
    ASSERT_TRUE(kuzu_value_is_null(value));
    kuzu_value_destroy(value);
}

TEST_F(CApiValueTest, SetNull) {
    kuzu_value* value = kuzu_value_create_int64(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    ASSERT_FALSE(kuzu_value_is_null(value));
    kuzu_value_set_null(value, true);
    ASSERT_TRUE(kuzu_value_is_null(value));
    kuzu_value_set_null(value, false);
    ASSERT_FALSE(kuzu_value_is_null(value));
    kuzu_value_destroy(value);
}

TEST_F(CApiValueTest, CreateDefault) {
    auto type = kuzu_data_type_create(kuzu_data_type_id::KUZU_INT64, nullptr, 0);
    kuzu_value* value = kuzu_value_create_default(type);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    kuzu_data_type_destroy(type);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT64);
    ASSERT_EQ(cppValue->getValue<int64_t>(), 0);
    kuzu_value_destroy(value);

    type = kuzu_data_type_create(kuzu_data_type_id::KUZU_STRING, nullptr, 0);
    value = kuzu_value_create_default(type);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    kuzu_data_type_destroy(type);
    cppValue = static_cast<Value*>(value->_value);
    ASSERT_FALSE(kuzu_value_is_null(value));
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(cppValue->getValue<std::string>(), "");
    kuzu_value_destroy(value);
}

TEST_F(CApiValueTest, CreateBool) {
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

TEST_F(CApiValueTest, CreateInt16) {
    kuzu_value* value = kuzu_value_create_int16(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT16);
    ASSERT_EQ(cppValue->getValue<int16_t>(), 123);
    kuzu_value_destroy(value);
}

TEST_F(CApiValueTest, CreateInt32) {
    kuzu_value* value = kuzu_value_create_int32(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT32);
    ASSERT_EQ(cppValue->getValue<int32_t>(), 123);
    kuzu_value_destroy(value);
}

TEST_F(CApiValueTest, CreateInt64) {
    kuzu_value* value = kuzu_value_create_int64(123);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::INT64);
    ASSERT_EQ(cppValue->getValue<int64_t>(), 123);
    kuzu_value_destroy(value);
}

TEST_F(CApiValueTest, CreateFloat) {
    kuzu_value* value = kuzu_value_create_float(123.456);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::FLOAT);
    ASSERT_FLOAT_EQ(cppValue->getValue<float>(), 123.456);
    kuzu_value_destroy(value);
}

TEST_F(CApiValueTest, CreateDouble) {
    kuzu_value* value = kuzu_value_create_double(123.456);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::DOUBLE);
    ASSERT_DOUBLE_EQ(cppValue->getValue<double>(), 123.456);
    kuzu_value_destroy(value);
}

TEST_F(CApiValueTest, CreateInternalID) {
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

TEST_F(CApiValueTest, CreateNodeVal) {
    auto internalID = kuzu_internal_id_t{1, 123};
    auto nodeVal = kuzu_node_val_create(internalID, (char*)"person");
    auto value = kuzu_value_create_node_val(nodeVal);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::NODE);
    auto nodeValCpp = cppValue->getValue<NodeVal>();
    ASSERT_EQ(nodeValCpp.getNodeID().tableID, 1);
    ASSERT_EQ(nodeValCpp.getNodeID().offset, 123);
    ASSERT_EQ(nodeValCpp.getLabelName(), "person");
    ASSERT_EQ(nodeValCpp.getProperties().size(), 0);
    kuzu_value_destroy(value);
    kuzu_node_val_destroy(nodeVal);
}

TEST_F(CApiValueTest, CreateRelVal) {
    auto srcID = kuzu_internal_id_t{1, 123};
    auto dstID = kuzu_internal_id_t{2, 456};
    auto relVal = kuzu_rel_val_create(srcID, dstID, (char*)"knows");
    auto value = kuzu_value_create_rel_val(relVal);
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::REL);
    auto relValCpp = cppValue->getValue<RelVal>();
    ASSERT_EQ(relValCpp.getSrcNodeID().tableID, 1);
    ASSERT_EQ(relValCpp.getSrcNodeID().offset, 123);
    ASSERT_EQ(relValCpp.getDstNodeID().tableID, 2);
    ASSERT_EQ(relValCpp.getDstNodeID().offset, 456);
    ASSERT_EQ(relValCpp.getLabelName(), "knows");
    ASSERT_EQ(relValCpp.getProperties().size(), 0);
    kuzu_value_destroy(value);
    kuzu_rel_val_destroy(relVal);
}

TEST_F(CApiValueTest, CreateDate) {
    kuzu_value* value = kuzu_value_create_date(kuzu_date_t{123});
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::DATE);
    auto cppDate = cppValue->getValue<date_t>();
    ASSERT_EQ(cppDate.days, 123);
    kuzu_value_destroy(value);
}

TEST_F(CApiValueTest, CreateTimeStamp) {
    kuzu_value* value = kuzu_value_create_timestamp(kuzu_timestamp_t{123});
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::TIMESTAMP);
    auto cppTimeStamp = cppValue->getValue<timestamp_t>();
    ASSERT_EQ(cppTimeStamp.value, 123);
    kuzu_value_destroy(value);
}

TEST_F(CApiValueTest, CreateInterval) {
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

TEST_F(CApiValueTest, CreateString) {
    kuzu_value* value = kuzu_value_create_string((char*)"abcdefg");
    ASSERT_FALSE(value->_is_owned_by_cpp);
    auto cppValue = static_cast<Value*>(value->_value);
    ASSERT_EQ(cppValue->getDataType().getLogicalTypeID(), LogicalTypeID::STRING);
    ASSERT_EQ(cppValue->getValue<std::string>(), "abcdefg");
    kuzu_value_destroy(value);
}

TEST_F(CApiValueTest, Clone) {
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

TEST_F(CApiValueTest, Copy) {
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
    ASSERT_EQ(kuzu_value_get_struct_num_fields(value), 4);

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
    ASSERT_STREQ(fieldName, "RATING");
    free(fieldName);

    fieldName = kuzu_value_get_struct_field_name(value, 1);
    ASSERT_STREQ(fieldName, "VIEWS");
    free(fieldName);

    fieldName = kuzu_value_get_struct_field_name(value, 2);
    ASSERT_STREQ(fieldName, "RELEASE");
    free(fieldName);

    fieldName = kuzu_value_get_struct_field_name(value, 3);
    ASSERT_STREQ(fieldName, "FILM");
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
    ASSERT_EQ(kuzu_data_type_get_id(fieldType), KUZU_INT64);
    ASSERT_EQ(kuzu_value_get_int64(fieldValue), 10003);
    kuzu_data_type_destroy(fieldType);
    kuzu_value_destroy(fieldValue);

    fieldValue = kuzu_value_get_struct_field_value(value, 2);
    fieldType = kuzu_value_get_data_type(fieldValue);
    ASSERT_EQ(kuzu_data_type_get_id(fieldType), KUZU_TIMESTAMP);
    auto timestamp = kuzu_value_get_timestamp(fieldValue);
    ASSERT_EQ(timestamp.value, 1297442662000000);
    kuzu_data_type_destroy(fieldType);
    kuzu_value_destroy(fieldValue);

    fieldValue = kuzu_value_get_struct_field_value(value, 3);
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
    auto node = kuzu_value_get_node_val(value);
    auto nodeIDVal = kuzu_node_val_get_id_val(node);
    auto internalID = kuzu_value_get_internal_id(nodeIDVal);
    ASSERT_EQ(internalID.table_id, 0);
    ASSERT_EQ(internalID.offset, 0);
    kuzu_value_destroy(nodeIDVal);
    kuzu_node_val_destroy(node);
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
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    auto rel = kuzu_value_get_rel_val(value);
    auto relSrcID = kuzu_rel_val_get_src_id(rel);
    ASSERT_EQ(relSrcID.table_id, 0);
    ASSERT_EQ(relSrcID.offset, 0);
    auto relDstID = kuzu_rel_val_get_dst_id(rel);
    ASSERT_EQ(relDstID.table_id, 0);
    ASSERT_EQ(relDstID.offset, 1);
    auto relLabel = kuzu_rel_val_get_label_name(rel);
    ASSERT_STREQ(relLabel, "knows");
    auto propertiesSize = kuzu_rel_val_get_property_size(rel);
    ASSERT_EQ(propertiesSize, 5);
    free(relLabel);
    kuzu_rel_val_destroy(rel);
    kuzu_value_destroy(value);
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

TEST_F(CApiValueTest, ToSting) {
    auto connection = getConnection();
    auto result = kuzu_connection_query(
        connection, (char*)"MATCH (a:person) RETURN a.fName, a.isStudent, a.workedHours");
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

TEST_F(CApiValueTest, NodeValClone) {
    auto internalID = kuzu_internal_id_t{1, 123};
    auto nodeVal = kuzu_node_val_create(internalID, (char*)"person");
    auto nodeValClone = kuzu_node_val_clone(nodeVal);
    kuzu_node_val_destroy(nodeVal);
    auto nodeValCpp = (NodeVal*)nodeValClone->_node_val;
    ASSERT_EQ(nodeValCpp->getNodeID().tableID, 1);
    ASSERT_EQ(nodeValCpp->getNodeID().offset, 123);
    ASSERT_EQ(nodeValCpp->getLabelName(), "person");
    ASSERT_EQ(nodeValCpp->getProperties().size(), 0);
    kuzu_node_val_destroy(nodeValClone);
}

TEST_F(CApiValueTest, NodeValGetLabelVal) {
    auto internalID = kuzu_internal_id_t{1, 123};
    auto nodeVal = kuzu_node_val_create(internalID, (char*)"person");
    auto value = kuzu_value_create_node_val(nodeVal);
    auto labelVal = kuzu_node_val_get_label_val(nodeVal);
    auto labelStr = kuzu_value_get_string(labelVal);
    ASSERT_STREQ(labelStr, "person");
    free(labelStr);
    kuzu_value_destroy(labelVal);
    kuzu_value_destroy(value);
    kuzu_node_val_destroy(nodeVal);
}

TEST_F(CApiValueTest, NodeValGetID) {
    auto internalID = kuzu_internal_id_t{1, 123};
    auto nodeVal = kuzu_node_val_create(internalID, (char*)"person");
    auto value = kuzu_value_create_node_val(nodeVal);
    auto nodeID = kuzu_node_val_get_id(nodeVal);
    ASSERT_EQ(nodeID.table_id, 1);
    ASSERT_EQ(nodeID.offset, 123);
    kuzu_value_destroy(value);
    kuzu_node_val_destroy(nodeVal);
}

TEST_F(CApiValueTest, NodeValGetLabelName) {
    auto internalID = kuzu_internal_id_t{1, 123};
    auto nodeVal = kuzu_node_val_create(internalID, (char*)"person");
    auto value = kuzu_value_create_node_val(nodeVal);
    auto labelName = kuzu_node_val_get_label_name(nodeVal);
    ASSERT_STREQ(labelName, "person");
    free(labelName);
    kuzu_value_destroy(value);
    kuzu_node_val_destroy(nodeVal);
}

TEST_F(CApiValueTest, NodeValAddProperty) {
    auto internalID = kuzu_internal_id_t{1, 123};
    auto nodeVal = kuzu_node_val_create(internalID, (char*)"person");
    auto propertySize = kuzu_node_val_get_property_size(nodeVal);
    ASSERT_EQ(propertySize, 0);

    auto propertyKey = (char*)"fName";
    auto propertyValue = kuzu_value_create_string((char*)"Alice");
    kuzu_node_val_add_property(nodeVal, propertyKey, propertyValue);
    propertySize = kuzu_node_val_get_property_size(nodeVal);
    ASSERT_EQ(propertySize, 1);
    kuzu_value_destroy(propertyValue);

    propertyKey = (char*)"age";
    propertyValue = kuzu_value_create_int64(10);
    kuzu_node_val_add_property(nodeVal, propertyKey, propertyValue);
    propertySize = kuzu_node_val_get_property_size(nodeVal);
    ASSERT_EQ(propertySize, 2);
    kuzu_value_destroy(propertyValue);

    kuzu_node_val_destroy(nodeVal);
}

TEST_F(CApiValueTest, NodeValGetProperty) {
    auto connection = getConnection();
    auto result =
        kuzu_connection_query(connection, (char*)"MATCH (a:person) RETURN a ORDER BY a.ID");
    ASSERT_TRUE(kuzu_query_result_is_success(result));
    ASSERT_TRUE(kuzu_query_result_has_next(result));
    auto flatTuple = kuzu_query_result_get_next(result);
    auto value = kuzu_flat_tuple_get_value(flatTuple, 0);
    ASSERT_TRUE(value->_is_owned_by_cpp);
    auto node = kuzu_value_get_node_val(value);
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

    kuzu_node_val_destroy(node);
    kuzu_value_destroy(value);
    kuzu_flat_tuple_destroy(flatTuple);
    kuzu_query_result_destroy(result);
}

TEST_F(CApiValueTest, NodeValToString) {
    auto internalID = kuzu_internal_id_t{1, 123};
    auto nodeVal = kuzu_node_val_create(internalID, (char*)"person");

    auto propertyKey = (char*)"fName";
    auto propertyValue = kuzu_value_create_string((char*)"Smith");
    kuzu_node_val_add_property(nodeVal, propertyKey, propertyValue);
    kuzu_value_destroy(propertyValue);

    propertyKey = (char*)"age";
    propertyValue = kuzu_value_create_int64(10);
    kuzu_node_val_add_property(nodeVal, propertyKey, propertyValue);
    kuzu_value_destroy(propertyValue);

    auto str = kuzu_node_val_to_string(nodeVal);
    ASSERT_STREQ(str, "(label:person, 1:123, {fName:Smith, age:10})");
    free(str);
    kuzu_node_val_destroy(nodeVal);
}

TEST_F(CApiValueTest, RelValClone) {
    auto srcID = kuzu_internal_id_t{1, 123};
    auto dstID = kuzu_internal_id_t{2, 456};
    auto relVal = kuzu_rel_val_create(srcID, dstID, (char*)"knows");
    auto relValClone = kuzu_rel_val_clone(relVal);
    kuzu_rel_val_destroy(relVal);
    auto srcIDVal = kuzu_rel_val_get_src_id_val(relValClone);
    auto dstIDVal = kuzu_rel_val_get_dst_id_val(relValClone);
    auto srcIDClone = kuzu_value_get_internal_id(srcIDVal);
    auto dstIDClone = kuzu_value_get_internal_id(dstIDVal);
    ASSERT_EQ(srcIDClone.table_id, srcID.table_id);
    ASSERT_EQ(srcIDClone.offset, srcID.offset);
    ASSERT_EQ(dstIDClone.table_id, dstID.table_id);
    ASSERT_EQ(dstIDClone.offset, dstID.offset);
    auto labelName = kuzu_rel_val_get_label_name(relValClone);
    ASSERT_STREQ(labelName, "knows");
    auto propertySize = kuzu_rel_val_get_property_size(relValClone);
    ASSERT_EQ(propertySize, 0);
    free(labelName);
    kuzu_value_destroy(srcIDVal);
    kuzu_value_destroy(dstIDVal);
    kuzu_rel_val_destroy(relValClone);
}

TEST_F(CApiValueTest, RelValAddAndGetProperty) {
    auto srcID = kuzu_internal_id_t{1, 123};
    auto dstID = kuzu_internal_id_t{2, 456};
    auto relVal = kuzu_rel_val_create(srcID, dstID, (char*)"knows");
    auto propertySize = kuzu_rel_val_get_property_size(relVal);
    ASSERT_EQ(propertySize, 0);

    auto propertyKey = (char*)"fName";
    auto propertyValue = kuzu_value_create_string((char*)"Alice");
    kuzu_rel_val_add_property(relVal, propertyKey, propertyValue);
    propertySize = kuzu_rel_val_get_property_size(relVal);
    ASSERT_EQ(propertySize, 1);
    kuzu_value_destroy(propertyValue);

    propertyKey = (char*)"age";
    propertyValue = kuzu_value_create_int64(10);
    kuzu_rel_val_add_property(relVal, propertyKey, propertyValue);
    propertySize = kuzu_rel_val_get_property_size(relVal);
    ASSERT_EQ(propertySize, 2);
    kuzu_value_destroy(propertyValue);

    propertyKey = kuzu_rel_val_get_property_name_at(relVal, 0);
    ASSERT_STREQ(propertyKey, "fName");
    free(propertyKey);

    propertyKey = kuzu_rel_val_get_property_name_at(relVal, 1);
    ASSERT_STREQ(propertyKey, "age");
    free(propertyKey);

    propertyValue = kuzu_rel_val_get_property_value_at(relVal, 0);
    auto propertyValuefName = kuzu_value_get_string(propertyValue);
    ASSERT_STREQ(propertyValuefName, "Alice");
    kuzu_value_destroy(propertyValue);
    free(propertyValuefName);

    propertyValue = kuzu_rel_val_get_property_value_at(relVal, 1);
    auto propertyValueAge = kuzu_value_get_int64(propertyValue);
    ASSERT_EQ(propertyValueAge, 10);
    kuzu_value_destroy(propertyValue);

    kuzu_rel_val_destroy(relVal);
}

TEST_F(CApiValueTest, RelValToString) {
    auto srcID = kuzu_internal_id_t{1, 123};
    auto dstID = kuzu_internal_id_t{2, 456};
    auto relVal = kuzu_rel_val_create(srcID, dstID, (char*)"knows");

    auto propertyKey = (char*)"fName";
    auto propertyValue = kuzu_value_create_string((char*)"Alice");
    kuzu_rel_val_add_property(relVal, propertyKey, propertyValue);
    kuzu_value_destroy(propertyValue);

    propertyKey = (char*)"age";
    propertyValue = kuzu_value_create_int64(10);
    kuzu_rel_val_add_property(relVal, propertyKey, propertyValue);
    kuzu_value_destroy(propertyValue);

    auto str = kuzu_rel_val_to_string(relVal);
    ASSERT_STREQ(str, "(1:123)-[label:knows, {fName:Alice, age:10}]->(2:456)");

    kuzu_rel_val_destroy(relVal);
    free(str);
}
