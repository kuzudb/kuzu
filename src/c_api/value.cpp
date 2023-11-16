#include "common/types/value/value.h"

#include "c_api/helpers.h"
#include "c_api/kuzu.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "common/types/value/nested.h"
#include "common/types/value/node.h"
#include "common/types/value/recursive_rel.h"
#include "common/types/value/rel.h"
#include "function/cast/functions/cast_from_string_functions.h"

using namespace kuzu::common;

kuzu_value* kuzu_value_create_null() {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    c_value->_value = new Value(Value::createNullValue());
    return c_value;
}

kuzu_value* kuzu_value_create_null_with_data_type(kuzu_logical_type* data_type) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    c_value->_value =
        new Value(Value::createNullValue(*static_cast<LogicalType*>(data_type->_data_type)));
    return c_value;
}

bool kuzu_value_is_null(kuzu_value* value) {
    return static_cast<Value*>(value->_value)->isNull();
}

void kuzu_value_set_null(kuzu_value* value, bool is_null) {
    static_cast<Value*>(value->_value)->setNull(is_null);
}

kuzu_value* kuzu_value_create_default(kuzu_logical_type* data_type) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    c_value->_value =
        new Value(Value::createDefaultValue(*static_cast<LogicalType*>(data_type->_data_type)));
    return c_value;
}

kuzu_value* kuzu_value_create_bool(bool val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    c_value->_value = new Value(val_);
    return c_value;
}

kuzu_value* kuzu_value_create_int8(int8_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    c_value->_value = new Value(val_);
    return c_value;
}

kuzu_value* kuzu_value_create_int16(int16_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    c_value->_value = new Value(val_);
    return c_value;
}

kuzu_value* kuzu_value_create_int32(int32_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    c_value->_value = new Value(val_);
    return c_value;
}

kuzu_value* kuzu_value_create_int64(int64_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    c_value->_value = new Value(val_);
    return c_value;
}

kuzu_value* kuzu_value_create_uint8(uint8_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    c_value->_value = new Value(val_);
    return c_value;
}

kuzu_value* kuzu_value_create_uint16(uint16_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    c_value->_value = new Value(val_);
    return c_value;
}

kuzu_value* kuzu_value_create_uint32(uint32_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    c_value->_value = new Value(val_);
    return c_value;
}

kuzu_value* kuzu_value_create_uint64(uint64_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    c_value->_value = new Value(val_);
    return c_value;
}

kuzu_value* kuzu_value_create_int128(kuzu_int128_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    int128_t int128(val_.low, val_.high);
    c_value->_value = new Value(int128);
    return c_value;
}

kuzu_value* kuzu_value_create_float(float val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    c_value->_value = new Value(val_);
    return c_value;
}

kuzu_value* kuzu_value_create_double(double val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    c_value->_value = new Value(val_);
    return c_value;
}

kuzu_value* kuzu_value_create_internal_id(kuzu_internal_id_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    internalID_t id(val_.offset, val_.table_id);
    c_value->_value = new Value(id);
    return c_value;
}

kuzu_value* kuzu_value_create_date(kuzu_date_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    auto date = date_t(val_.days);
    c_value->_value = new Value(date);
    return c_value;
}

kuzu_value* kuzu_value_create_timestamp(kuzu_timestamp_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    auto timestamp = timestamp_t(val_.value);
    c_value->_value = new Value(timestamp);
    return c_value;
}

kuzu_value* kuzu_value_create_interval(kuzu_interval_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    auto interval = interval_t(val_.months, val_.days, val_.micros);
    c_value->_value = new Value(interval);
    return c_value;
}

kuzu_value* kuzu_value_create_string(const char* val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    c_value->_value = new Value(val_);
    return c_value;
}

kuzu_value* kuzu_value_clone(kuzu_value* value) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    c_value->_value = new Value(*static_cast<Value*>(value->_value));
    return c_value;
}

void kuzu_value_copy(kuzu_value* value, kuzu_value* other) {
    static_cast<Value*>(value->_value)->copyValueFrom(*static_cast<Value*>(other->_value));
}

void kuzu_value_destroy(kuzu_value* value) {
    if (value == nullptr) {
        return;
    }
    if ((value->_value != nullptr) && (!value->_is_owned_by_cpp)) {
        delete static_cast<Value*>(value->_value);
    }
    free(value);
}

uint64_t kuzu_value_get_list_size(kuzu_value* value) {
    return NestedVal::getChildrenSize(static_cast<Value*>(value->_value));
}

kuzu_value* kuzu_value_get_list_element(kuzu_value* value, uint64_t index) {
    auto listValue = static_cast<Value*>(value->_value);
    if (index >= NestedVal::getChildrenSize(listValue)) {
        return nullptr;
    }
    auto val = NestedVal::getChildVal(listValue, index);
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_value = val;
    c_value->_is_owned_by_cpp = true;
    return c_value;
}

uint64_t kuzu_value_get_struct_num_fields(kuzu_value* value) {
    auto val = static_cast<Value*>(value->_value);
    auto data_type = val->getDataType();
    return StructType::getNumFields(data_type);
}

char* kuzu_value_get_struct_field_name(kuzu_value* value, uint64_t index) {
    auto val = static_cast<Value*>(value->_value);
    auto data_type = val->getDataType();
    return convertToOwnedCString(StructType::getFields(data_type)[index]->getName());
}

kuzu_value* kuzu_value_get_struct_field_value(kuzu_value* value, uint64_t index) {
    return kuzu_value_get_list_element(value, index);
}

kuzu_value* kuzu_value_get_recursive_rel_node_list(kuzu_value* value) {
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_is_owned_by_cpp = true;
    c_value->_value = RecursiveRelVal::getNodes(static_cast<Value*>(value->_value));
    return c_value;
}

kuzu_value* kuzu_value_get_recursive_rel_rel_list(kuzu_value* value) {
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_is_owned_by_cpp = true;
    c_value->_value = RecursiveRelVal::getRels(static_cast<Value*>(value->_value));
    return c_value;
}

kuzu_logical_type* kuzu_value_get_data_type(kuzu_value* value) {
    auto* c_data_type = (kuzu_logical_type*)malloc(sizeof(kuzu_logical_type));
    c_data_type->_data_type = new LogicalType(*static_cast<Value*>(value->_value)->getDataType());
    return c_data_type;
}

bool kuzu_value_get_bool(kuzu_value* value) {
    return static_cast<Value*>(value->_value)->getValue<bool>();
}

int8_t kuzu_value_get_int8(kuzu_value* value) {
    return static_cast<Value*>(value->_value)->getValue<int8_t>();
}

int16_t kuzu_value_get_int16(kuzu_value* value) {
    return static_cast<Value*>(value->_value)->getValue<int16_t>();
}

int32_t kuzu_value_get_int32(kuzu_value* value) {
    return static_cast<Value*>(value->_value)->getValue<int32_t>();
}

int64_t kuzu_value_get_int64(kuzu_value* value) {
    return static_cast<Value*>(value->_value)->getValue<int64_t>();
}

uint8_t kuzu_value_get_uint8(kuzu_value* value) {
    return static_cast<Value*>(value->_value)->getValue<uint8_t>();
}

uint16_t kuzu_value_get_uint16(kuzu_value* value) {
    return static_cast<Value*>(value->_value)->getValue<uint16_t>();
}

uint32_t kuzu_value_get_uint32(kuzu_value* value) {
    return static_cast<Value*>(value->_value)->getValue<uint32_t>();
}

uint64_t kuzu_value_get_uint64(kuzu_value* value) {
    return static_cast<Value*>(value->_value)->getValue<uint64_t>();
}

kuzu_int128_t kuzu_value_get_int128(kuzu_value* value) {
    auto int128_val = static_cast<Value*>(value->_value)->getValue<int128_t>();
    kuzu_int128_t c_int128;
    c_int128.low = int128_val.low;
    c_int128.high = int128_val.high;
    return c_int128;
}

kuzu_int128_t kuzu_int128_t_from_string(const char* str) {
    int128_t int128_val = 0;
    kuzu_int128_t c_int128;
    try {
        kuzu::function::CastString::operation(ku_string_t{str, strlen(str)}, int128_val);
        c_int128.low = int128_val.low;
        c_int128.high = int128_val.high;
    } catch (ConversionException& e) {
        c_int128.low = 0;
        c_int128.high = 0;
    }
    return c_int128;
}

char* kuzu_int128_t_to_string(kuzu_int128_t int128_val) {
    int128_t c_int128;
    c_int128.low = int128_val.low;
    c_int128.high = int128_val.high;
    return convertToOwnedCString(TypeUtils::toString(c_int128));
}
// TODO: bind all int128_t supported functions

float kuzu_value_get_float(kuzu_value* value) {
    return static_cast<Value*>(value->_value)->getValue<float>();
}

double kuzu_value_get_double(kuzu_value* value) {
    return static_cast<Value*>(value->_value)->getValue<double>();
}

kuzu_internal_id_t kuzu_value_get_internal_id(kuzu_value* value) {
    auto id = static_cast<Value*>(value->_value)->getValue<internalID_t>();
    kuzu_internal_id_t c_id;
    c_id.offset = id.offset;
    c_id.table_id = id.tableID;
    return c_id;
}

kuzu_date_t kuzu_value_get_date(kuzu_value* value) {
    auto date_val = static_cast<Value*>(value->_value)->getValue<date_t>();
    kuzu_date_t c_date;
    c_date.days = date_val.days;
    return c_date;
}

kuzu_timestamp_t kuzu_value_get_timestamp(kuzu_value* value) {
    auto timestamp_val = static_cast<Value*>(value->_value)->getValue<timestamp_t>();
    kuzu_timestamp_t c_timestamp;
    c_timestamp.value = timestamp_val.value;
    return c_timestamp;
}

kuzu_interval_t kuzu_value_get_interval(kuzu_value* value) {
    auto interval_val = static_cast<Value*>(value->_value)->getValue<interval_t>();
    kuzu_interval_t c_interval;
    c_interval.months = interval_val.months;
    c_interval.days = interval_val.days;
    c_interval.micros = interval_val.micros;
    return c_interval;
}

char* kuzu_value_get_string(kuzu_value* value) {
    return convertToOwnedCString(static_cast<Value*>(value->_value)->getValue<std::string>());
}

uint8_t* kuzu_value_get_blob(kuzu_value* value) {
    return (uint8_t*)convertToOwnedCString(
        static_cast<Value*>(value->_value)->getValue<std::string>());
}

char* kuzu_value_to_string(kuzu_value* value) {
    return convertToOwnedCString(static_cast<Value*>(value->_value)->toString());
}

kuzu_value* kuzu_node_val_get_id_val(kuzu_value* node_val) {
    auto id_val = NodeVal::getNodeIDVal(static_cast<Value*>(node_val->_value));
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_value = id_val;
    c_value->_is_owned_by_cpp = true;
    return c_value;
}

kuzu_value* kuzu_node_val_get_label_val(kuzu_value* node_val) {
    auto label_val = NodeVal::getLabelVal(static_cast<Value*>(node_val->_value));
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_value = label_val;
    c_value->_is_owned_by_cpp = true;
    return c_value;
}

uint64_t kuzu_node_val_get_property_size(kuzu_value* node_val) {
    return NodeVal::getNumProperties(static_cast<Value*>(node_val->_value));
}

char* kuzu_node_val_get_property_name_at(kuzu_value* node_val, uint64_t index) {
    return convertToOwnedCString(
        NodeVal::getPropertyName(static_cast<Value*>(node_val->_value), index));
}

kuzu_value* kuzu_node_val_get_property_value_at(kuzu_value* node_val, uint64_t index) {
    auto value = NodeVal::getPropertyVal(static_cast<Value*>(node_val->_value), index);
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_value = value;
    c_value->_is_owned_by_cpp = true;
    return c_value;
}

char* kuzu_node_val_to_string(kuzu_value* node_val) {
    return convertToOwnedCString(NodeVal::toString(static_cast<Value*>(node_val->_value)));
}

kuzu_value* kuzu_rel_val_get_src_id_val(kuzu_value* rel_val) {
    auto src_id_val = RelVal::getSrcNodeIDVal(static_cast<Value*>(rel_val->_value));
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_value = src_id_val;
    c_value->_is_owned_by_cpp = true;
    return c_value;
}

kuzu_value* kuzu_rel_val_get_dst_id_val(kuzu_value* rel_val) {
    auto dst_id_val = RelVal::getDstNodeIDVal(static_cast<Value*>(rel_val->_value));
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_value = dst_id_val;
    c_value->_is_owned_by_cpp = true;
    return c_value;
}

kuzu_value* kuzu_rel_val_get_label_val(kuzu_value* rel_val) {
    auto label_val = RelVal::getLabelVal(static_cast<Value*>(rel_val->_value));
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_value = label_val;
    c_value->_is_owned_by_cpp = true;
    return c_value;
}

uint64_t kuzu_rel_val_get_property_size(kuzu_value* rel_val) {
    return RelVal::getNumProperties(static_cast<Value*>(rel_val->_value));
}
char* kuzu_rel_val_get_property_name_at(kuzu_value* rel_val, uint64_t index) {
    return convertToOwnedCString(
        RelVal::getPropertyName(static_cast<Value*>(rel_val->_value), index));
}

kuzu_value* kuzu_rel_val_get_property_value_at(kuzu_value* rel_val, uint64_t index) {
    auto value = RelVal::getPropertyVal(static_cast<Value*>(rel_val->_value), index);
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_value = value;
    c_value->_is_owned_by_cpp = true;
    return c_value;
}

char* kuzu_rel_val_to_string(kuzu_value* rel_val) {
    return convertToOwnedCString(RelVal::toString(static_cast<Value*>(rel_val->_value)));
}
