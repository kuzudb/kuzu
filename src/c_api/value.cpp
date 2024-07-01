#include "common/types/value/value.h"

#include "c_api/helpers.h"
#include "c_api/kuzu.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "common/types/value/nested.h"
#include "common/types/value/node.h"
#include "common/types/value/rdf_variant.h"
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

kuzu_value* kuzu_value_create_timestamp_ns(kuzu_timestamp_ns_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    auto timestamp_ns = timestamp_ns_t(val_.value);
    c_value->_value = new Value(timestamp_ns);
    return c_value;
}

kuzu_value* kuzu_value_create_timestamp_ms(kuzu_timestamp_ms_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    auto timestamp_ms = timestamp_ms_t(val_.value);
    c_value->_value = new Value(timestamp_ms);
    return c_value;
}

kuzu_value* kuzu_value_create_timestamp_sec(kuzu_timestamp_sec_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    auto timestamp_sec = timestamp_sec_t(val_.value);
    c_value->_value = new Value(timestamp_sec);
    return c_value;
}

kuzu_value* kuzu_value_create_timestamp_tz(kuzu_timestamp_tz_t val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    auto timestamp_tz = timestamp_tz_t(val_.value);
    c_value->_value = new Value(timestamp_tz);
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
    if (!value->_is_owned_by_cpp) {
        if (value->_value != nullptr) {
            delete static_cast<Value*>(value->_value);
        }
        free(value);
    }
}

kuzu_state kuzu_value_get_list_size(kuzu_value* value, uint64_t* out_result) {
    if (static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID() !=
        LogicalTypeID::LIST) {
        return KuzuError;
    }
    *out_result = NestedVal::getChildrenSize(static_cast<Value*>(value->_value));
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_list_element(kuzu_value* value, uint64_t index, kuzu_value* out_value) {
    auto physical_type_id = static_cast<Value*>(value->_value)->getDataType().getPhysicalType();
    if (physical_type_id != PhysicalTypeID::ARRAY && physical_type_id != PhysicalTypeID::STRUCT &&
        physical_type_id != PhysicalTypeID::LIST) {
        return KuzuError;
    }
    auto listValue = static_cast<Value*>(value->_value);
    if (index >= NestedVal::getChildrenSize(listValue)) {
        return KuzuError;
    }
    try {
        auto val = NestedVal::getChildVal(listValue, index);
        out_value->_value = val;
        out_value->_is_owned_by_cpp = true;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_struct_num_fields(kuzu_value* value, uint64_t* out_result) {
    auto physical_type_id = static_cast<Value*>(value->_value)->getDataType().getPhysicalType();
    if (physical_type_id != PhysicalTypeID::STRUCT) {
        return KuzuError;
    }
    auto val = static_cast<Value*>(value->_value);
    const auto& data_type = val->getDataType();
    try {
        *out_result = StructType::getNumFields(data_type);
        return KuzuSuccess;
    } catch (Exception& e) {
        return KuzuError;
    }
}

kuzu_state kuzu_value_get_struct_field_name(kuzu_value* value, uint64_t index, char** out_result) {
    auto physical_type_id = static_cast<Value*>(value->_value)->getDataType().getPhysicalType();
    if (physical_type_id != PhysicalTypeID::STRUCT) {
        return KuzuError;
    }
    auto val = static_cast<Value*>(value->_value);
    const auto& data_type = val->getDataType();
    if (index >= StructType::getNumFields(data_type)) {
        return KuzuError;
    }
    std::string struct_field_name = StructType::getFields(data_type)[index].getName();
    if (struct_field_name.empty()) {
        return KuzuError;
    }
    *out_result = convertToOwnedCString(struct_field_name);
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_struct_field_value(kuzu_value* value, uint64_t index,
    kuzu_value* out_value) {
    return kuzu_value_get_list_element(value, index, out_value);
}

kuzu_state kuzu_value_get_recursive_rel_node_list(kuzu_value* value, kuzu_value* out_value) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RECURSIVE_REL) {
        return KuzuError;
    }
    out_value->_is_owned_by_cpp = true;
    try {
        out_value->_value = RecursiveRelVal::getNodes(static_cast<Value*>(value->_value));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_recursive_rel_rel_list(kuzu_value* value, kuzu_value* out_value) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RECURSIVE_REL) {
        return KuzuError;
    }
    out_value->_is_owned_by_cpp = true;
    try {
        out_value->_value = RecursiveRelVal::getRels(static_cast<Value*>(value->_value));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

void kuzu_value_get_data_type(kuzu_value* value, kuzu_logical_type* out_data_type) {
    out_data_type->_data_type =
        new LogicalType(static_cast<Value*>(value->_value)->getDataType().copy());
}

kuzu_state kuzu_value_get_bool(kuzu_value* value, bool* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::BOOL) {
        return KuzuError;
    }
    try {
        *out_result = static_cast<Value*>(value->_value)->getValue<bool>();
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_int8(kuzu_value* value, int8_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::INT8) {
        return KuzuError;
    }
    try {
        *out_result = static_cast<Value*>(value->_value)->getValue<int8_t>();
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_int16(kuzu_value* value, int16_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::INT16) {
        return KuzuError;
    }
    try {
        *out_result = static_cast<Value*>(value->_value)->getValue<int16_t>();
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_int32(kuzu_value* value, int32_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::INT32) {
        return KuzuError;
    }
    try {
        *out_result = static_cast<Value*>(value->_value)->getValue<int32_t>();
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_int64(kuzu_value* value, int64_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::INT64) {
        return KuzuError;
    }
    try {
        *out_result = static_cast<Value*>(value->_value)->getValue<int64_t>();
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_uint8(kuzu_value* value, uint8_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::UINT8) {
        return KuzuError;
    }
    try {
        *out_result = static_cast<Value*>(value->_value)->getValue<uint8_t>();
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_uint16(kuzu_value* value, uint16_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::UINT16) {
        return KuzuError;
    }
    try {
        *out_result = static_cast<Value*>(value->_value)->getValue<uint16_t>();
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_uint32(kuzu_value* value, uint32_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::UINT32) {
        return KuzuError;
    }
    try {
        *out_result = static_cast<Value*>(value->_value)->getValue<uint32_t>();
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_uint64(kuzu_value* value, uint64_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::UINT64) {
        return KuzuError;
    }
    try {
        *out_result = static_cast<Value*>(value->_value)->getValue<uint64_t>();
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_int128(kuzu_value* value, kuzu_int128_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::INT128) {
        return KuzuError;
    }
    try {
        auto int128_val = static_cast<Value*>(value->_value)->getValue<int128_t>();
        out_result->low = int128_val.low;
        out_result->high = int128_val.high;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_int128_t_from_string(const char* str, kuzu_int128_t* out_result) {
    int128_t int128_val = 0;
    try {
        kuzu::function::CastString::operation(ku_string_t{str, strlen(str)}, int128_val);
        out_result->low = int128_val.low;
        out_result->high = int128_val.high;
    } catch (ConversionException& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_int128_t_to_string(kuzu_int128_t int128_val, char** out_result) {
    int128_t c_int128;
    c_int128.low = int128_val.low;
    c_int128.high = int128_val.high;
    try {
        *out_result = convertToOwnedCString(TypeUtils::toString(c_int128));
    } catch (ConversionException& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}
// TODO: bind all int128_t supported functions

kuzu_state kuzu_value_get_float(kuzu_value* value, float* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::FLOAT) {
        return KuzuError;
    }
    try {
        *out_result = static_cast<Value*>(value->_value)->getValue<float>();
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_double(kuzu_value* value, double* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::DOUBLE) {
        return KuzuError;
    }
    try {
        *out_result = static_cast<Value*>(value->_value)->getValue<double>();
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_internal_id(kuzu_value* value, kuzu_internal_id_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::INTERNAL_ID) {
        return KuzuError;
    }
    try {
        auto id = static_cast<Value*>(value->_value)->getValue<internalID_t>();
        out_result->offset = id.offset;
        out_result->table_id = id.tableID;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_date(kuzu_value* value, kuzu_date_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::DATE) {
        return KuzuError;
    }
    try {
        auto date_val = static_cast<Value*>(value->_value)->getValue<date_t>();
        out_result->days = date_val.days;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_timestamp(kuzu_value* value, kuzu_timestamp_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::TIMESTAMP) {
        return KuzuError;
    }
    try {
        auto timestamp_val = static_cast<Value*>(value->_value)->getValue<timestamp_t>();
        out_result->value = timestamp_val.value;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_timestamp_ns(kuzu_value* value, kuzu_timestamp_ns_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::TIMESTAMP_NS) {
        return KuzuError;
    }
    try {
        auto timestamp_val = static_cast<Value*>(value->_value)->getValue<timestamp_ns_t>();
        out_result->value = timestamp_val.value;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_timestamp_ms(kuzu_value* value, kuzu_timestamp_ms_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::TIMESTAMP_MS) {
        return KuzuError;
    }
    try {
        auto timestamp_val = static_cast<Value*>(value->_value)->getValue<timestamp_ms_t>();
        out_result->value = timestamp_val.value;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_timestamp_sec(kuzu_value* value, kuzu_timestamp_sec_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::TIMESTAMP_SEC) {
        return KuzuError;
    }
    try {
        auto timestamp_val = static_cast<Value*>(value->_value)->getValue<timestamp_sec_t>();
        out_result->value = timestamp_val.value;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_timestamp_tz(kuzu_value* value, kuzu_timestamp_tz_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::TIMESTAMP_TZ) {
        return KuzuError;
    }
    try {
        auto timestamp_val = static_cast<Value*>(value->_value)->getValue<timestamp_tz_t>();
        out_result->value = timestamp_val.value;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_interval(kuzu_value* value, kuzu_interval_t* out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::INTERVAL) {
        return KuzuError;
    }
    try {
        auto interval_val = static_cast<Value*>(value->_value)->getValue<interval_t>();
        out_result->months = interval_val.months;
        out_result->days = interval_val.days;
        out_result->micros = interval_val.micros;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_string(kuzu_value* value, char** out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::STRING) {
        return KuzuError;
    }
    try {
        *out_result =
            convertToOwnedCString(static_cast<Value*>(value->_value)->getValue<std::string>());
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_blob(kuzu_value* value, uint8_t** out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::BLOB) {
        return KuzuError;
    }
    try {
        auto blob = static_cast<Value*>(value->_value)->getValue<std::string>();
        *out_result = (uint8_t*)convertToOwnedCString(blob);
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_value_get_uuid(kuzu_value* value, char** out_result) {
    auto logical_type_id = static_cast<Value*>(value->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::UUID) {
        return KuzuError;
    }
    try {
        *out_result =
            convertToOwnedCString(static_cast<Value*>(value->_value)->getValue<std::string>());
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

char* kuzu_value_to_string(kuzu_value* value) {
    return convertToOwnedCString(static_cast<Value*>(value->_value)->toString());
}

kuzu_state kuzu_node_val_get_id_val(kuzu_value* node_val, kuzu_value* out_value) {
    auto logical_type_id = static_cast<Value*>(node_val->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::NODE) {
        return KuzuError;
    }
    try {
        auto id_val = NodeVal::getNodeIDVal(static_cast<Value*>(node_val->_value));
        out_value->_value = id_val;
        out_value->_is_owned_by_cpp = true;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_node_val_get_label_val(kuzu_value* node_val, kuzu_value* out_value) {
    auto logical_type_id = static_cast<Value*>(node_val->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::NODE) {
        return KuzuError;
    }
    try {
        auto label_val = NodeVal::getLabelVal(static_cast<Value*>(node_val->_value));
        out_value->_value = label_val;
        out_value->_is_owned_by_cpp = true;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_node_val_get_property_size(kuzu_value* node_val, uint64_t* out_result) {
    auto logical_type_id = static_cast<Value*>(node_val->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::NODE) {
        return KuzuError;
    }
    try {
        *out_result = NodeVal::getNumProperties(static_cast<Value*>(node_val->_value));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_node_val_get_property_name_at(kuzu_value* node_val, uint64_t index,
    char** out_result) {
    auto logical_type_id = static_cast<Value*>(node_val->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::NODE) {
        return KuzuError;
    }
    try {
        std::string property_name =
            NodeVal::getPropertyName(static_cast<Value*>(node_val->_value), index);
        if (property_name.empty()) {
            return KuzuError;
        }
        *out_result = convertToOwnedCString(property_name);
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_node_val_get_property_value_at(kuzu_value* node_val, uint64_t index,
    kuzu_value* out_value) {
    auto logical_type_id = static_cast<Value*>(node_val->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::NODE) {
        return KuzuError;
    }
    try {
        auto value = NodeVal::getPropertyVal(static_cast<Value*>(node_val->_value), index);
        out_value->_value = value;
        out_value->_is_owned_by_cpp = true;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_node_val_to_string(kuzu_value* node_val, char** out_result) {
    auto logical_type_id = static_cast<Value*>(node_val->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::NODE) {
        return KuzuError;
    }
    try {
        *out_result =
            convertToOwnedCString(NodeVal::toString(static_cast<Value*>(node_val->_value)));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rel_val_get_src_id_val(kuzu_value* rel_val, kuzu_value* out_value) {
    auto logical_type_id = static_cast<Value*>(rel_val->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::REL) {
        return KuzuError;
    }
    try {
        auto src_id_val = RelVal::getSrcNodeIDVal(static_cast<Value*>(rel_val->_value));
        out_value->_value = src_id_val;
        out_value->_is_owned_by_cpp = true;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rel_val_get_dst_id_val(kuzu_value* rel_val, kuzu_value* out_value) {
    auto logical_type_id = static_cast<Value*>(rel_val->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::REL) {
        return KuzuError;
    }
    try {
        auto dst_id_val = RelVal::getDstNodeIDVal(static_cast<Value*>(rel_val->_value));
        out_value->_value = dst_id_val;
        out_value->_is_owned_by_cpp = true;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rel_val_get_label_val(kuzu_value* rel_val, kuzu_value* out_value) {
    auto logical_type_id = static_cast<Value*>(rel_val->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::REL) {
        return KuzuError;
    }
    try {
        auto label_val = RelVal::getLabelVal(static_cast<Value*>(rel_val->_value));
        out_value->_value = label_val;
        out_value->_is_owned_by_cpp = true;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rel_val_get_property_size(kuzu_value* rel_val, uint64_t* out_result) {
    auto logical_type_id = static_cast<Value*>(rel_val->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::REL) {
        return KuzuError;
    }
    try {
        *out_result = RelVal::getNumProperties(static_cast<Value*>(rel_val->_value));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}
kuzu_state kuzu_rel_val_get_property_name_at(kuzu_value* rel_val, uint64_t index,
    char** out_result) {
    auto logical_type_id = static_cast<Value*>(rel_val->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::REL) {
        return KuzuError;
    }
    try {
        std::string property_name =
            RelVal::getPropertyName(static_cast<Value*>(rel_val->_value), index);
        if (property_name.empty()) {
            return KuzuError;
        }
        *out_result = convertToOwnedCString(property_name);
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rel_val_get_property_value_at(kuzu_value* rel_val, uint64_t index,
    kuzu_value* out_value) {
    auto logical_type_id = static_cast<Value*>(rel_val->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::REL) {
        return KuzuError;
    }
    try {
        auto value = RelVal::getPropertyVal(static_cast<Value*>(rel_val->_value), index);
        out_value->_value = value;
        out_value->_is_owned_by_cpp = true;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rel_val_to_string(kuzu_value* rel_val, char** out_result) {
    auto logical_type_id = static_cast<Value*>(rel_val->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::REL) {
        return KuzuError;
    }
    try {
        *out_result = convertToOwnedCString(RelVal::toString(static_cast<Value*>(rel_val->_value)));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_type(kuzu_value* rdf_variant, kuzu_data_type_id* out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        auto type = NestedVal::getChildVal(static_cast<Value*>(rdf_variant->_value), 0)
                        ->getValue<uint8_t>();
        *out_result = static_cast<kuzu_data_type_id>(type);
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_string(kuzu_value* rdf_variant, char** out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        auto str = RdfVariant::getValue<std::string>(static_cast<Value*>(rdf_variant->_value));
        *out_result = convertToOwnedCString(str);
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_blob(kuzu_value* rdf_variant, uint8_t** out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        auto blobData = RdfVariant::getValue<blob_t>(static_cast<Value*>(rdf_variant->_value));
        auto blobStr = blobData.value.getAsString();
        *out_result = (uint8_t*)convertToOwnedCString(blobStr);
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_int64(kuzu_value* rdf_variant, int64_t* out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        *out_result = RdfVariant::getValue<int64_t>(static_cast<Value*>(rdf_variant->_value));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_int32(kuzu_value* rdf_variant, int32_t* out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        *out_result = RdfVariant::getValue<int32_t>(static_cast<Value*>(rdf_variant->_value));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_int16(kuzu_value* rdf_variant, int16_t* out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        *out_result = RdfVariant::getValue<int16_t>(static_cast<Value*>(rdf_variant->_value));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_int8(kuzu_value* rdf_variant, int8_t* out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        *out_result = RdfVariant::getValue<int8_t>(static_cast<Value*>(rdf_variant->_value));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_uint64(kuzu_value* rdf_variant, uint64_t* out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        *out_result = RdfVariant::getValue<uint64_t>(static_cast<Value*>(rdf_variant->_value));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_uint32(kuzu_value* rdf_variant, uint32_t* out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        *out_result = RdfVariant::getValue<uint32_t>(static_cast<Value*>(rdf_variant->_value));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_uint16(kuzu_value* rdf_variant, uint16_t* out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        *out_result = RdfVariant::getValue<uint16_t>(static_cast<Value*>(rdf_variant->_value));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_uint8(kuzu_value* rdf_variant, uint8_t* out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        *out_result = RdfVariant::getValue<uint8_t>(static_cast<Value*>(rdf_variant->_value));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_float(kuzu_value* rdf_variant, float* out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        *out_result = RdfVariant::getValue<float>(static_cast<Value*>(rdf_variant->_value));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_double(kuzu_value* rdf_variant, double* out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        *out_result = RdfVariant::getValue<double>(static_cast<Value*>(rdf_variant->_value));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_bool(kuzu_value* rdf_variant, bool* out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        *out_result = RdfVariant::getValue<bool>(static_cast<Value*>(rdf_variant->_value));
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_date(kuzu_value* rdf_variant, kuzu_date_t* out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        auto dateVal = RdfVariant::getValue<date_t>(static_cast<Value*>(rdf_variant->_value));
        out_result->days = dateVal.days;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_timestamp(kuzu_value* rdf_variant, kuzu_timestamp_t* out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        auto timestampVal =
            RdfVariant::getValue<timestamp_t>(static_cast<Value*>(rdf_variant->_value));
        out_result->value = timestampVal.value;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

kuzu_state kuzu_rdf_variant_get_interval(kuzu_value* rdf_variant, kuzu_interval_t* out_result) {
    auto logical_type_id =
        static_cast<Value*>(rdf_variant->_value)->getDataType().getLogicalTypeID();
    if (logical_type_id != LogicalTypeID::RDF_VARIANT) {
        return KuzuError;
    }
    try {
        auto intervalVal =
            RdfVariant::getValue<interval_t>(static_cast<Value*>(rdf_variant->_value));
        out_result->months = intervalVal.months;
        out_result->days = intervalVal.days;
        out_result->micros = intervalVal.micros;
    } catch (Exception& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

void kuzu_destroy_string(char* str) {
    free(str);
}

void kuzu_destroy_blob(uint8_t* blob) {
    free(blob);
}

kuzu_state kuzu_timestamp_ns_to_tm(kuzu_timestamp_ns_t timestamp, struct tm* out_result) {
    time_t time = timestamp.value / 1000000000;
#ifdef _WIN32
    if (convertTimeToTm(time, out_result) != 0) {
        return KuzuError;
    }
#else
    if (gmtime_r(&time, out_result) == nullptr) {
        return KuzuError;
    }
#endif
    return KuzuSuccess;
}

kuzu_state kuzu_timestamp_ms_to_tm(kuzu_timestamp_ms_t timestamp, struct tm* out_result) {
    time_t time = timestamp.value / 1000;
#ifdef _WIN32
    if (convertTimeToTm(time, out_result) != 0) {
        return KuzuError;
    }
#else
    if (gmtime_r(&time, out_result) == nullptr) {
        return KuzuError;
    }
#endif
    return KuzuSuccess;
}

kuzu_state kuzu_timestamp_sec_to_tm(kuzu_timestamp_sec_t timestamp, struct tm* out_result) {
    time_t time = timestamp.value;
#ifdef _WIN32
    if (convertTimeToTm(time, out_result) != 0) {
        return KuzuError;
    }
#else
    if (gmtime_r(&time, out_result) == nullptr) {
        return KuzuError;
    }
#endif
    return KuzuSuccess;
}

kuzu_state kuzu_timestamp_tz_to_tm(kuzu_timestamp_tz_t timestamp, struct tm* out_result) {
    time_t time = timestamp.value / 1000000;
#ifdef _WIN32
    if (convertTimeToTm(time, out_result) != 0) {
        return KuzuError;
    }
#else
    if (gmtime_r(&time, out_result) == nullptr) {
        return KuzuError;
    }
#endif
    return KuzuSuccess;
}

kuzu_state kuzu_timestamp_to_tm(kuzu_timestamp_t timestamp, struct tm* out_result) {
    time_t time = timestamp.value / 1000000;
#ifdef _WIN32
    if (convertTimeToTm(time, out_result) != 0) {
        return KuzuError;
    }
#else
    if (gmtime_r(&time, out_result) == nullptr) {
        return KuzuError;
    }
#endif
    return KuzuSuccess;
}

kuzu_state kuzu_timestamp_ns_from_tm(struct tm tm, kuzu_timestamp_ns_t* out_result) {
#ifdef _WIN32
    int64_t time = convertTmToTime(tm);
#else
    int64_t time = timegm(&tm);
#endif
    if (time == -1) {
        return KuzuError;
    }
    out_result->value = time * 1000000000;
    return KuzuSuccess;
}

kuzu_state kuzu_timestamp_ms_from_tm(struct tm tm, kuzu_timestamp_ms_t* out_result) {
#ifdef _WIN32
    int64_t time = convertTmToTime(tm);
#else
    int64_t time = timegm(&tm);
#endif
    if (time == -1) {
        return KuzuError;
    }
    out_result->value = time * 1000;
    return KuzuSuccess;
}

kuzu_state kuzu_timestamp_sec_from_tm(struct tm tm, kuzu_timestamp_sec_t* out_result) {
#ifdef _WIN32
    int64_t time = convertTmToTime(tm);
#else
    int64_t time = timegm(&tm);
#endif
    if (time == -1) {
        return KuzuError;
    }
    out_result->value = time;
    return KuzuSuccess;
}

kuzu_state kuzu_timestamp_tz_from_tm(struct tm tm, kuzu_timestamp_tz_t* out_result) {
#ifdef _WIN32
    int64_t time = convertTmToTime(tm);
#else
    int64_t time = timegm(&tm);
#endif
    if (time == -1) {
        return KuzuError;
    }
    out_result->value = time * 1000000;
    return KuzuSuccess;
}

kuzu_state kuzu_timestamp_from_tm(struct tm tm, kuzu_timestamp_t* out_result) {
#ifdef _WIN32
    int64_t time = convertTmToTime(tm);
#else
    int64_t time = timegm(&tm);
#endif
    if (time == -1) {
        return KuzuError;
    }
    out_result->value = time * 1000000;
    return KuzuSuccess;
}

kuzu_state kuzu_date_to_tm(kuzu_date_t date, struct tm* out_result) {
    time_t time = date.days * 86400;
#ifdef _WIN32
    if (convertTimeToTm(time, out_result) != 0) {
        return KuzuError;
    }
#else
    if (gmtime_r(&time, out_result) == nullptr) {
        return KuzuError;
    }
#endif
    out_result->tm_hour = 0;
    out_result->tm_min = 0;
    out_result->tm_sec = 0;
    return KuzuSuccess;
}

kuzu_state kuzu_date_from_tm(struct tm tm, kuzu_date_t* out_result) {
#ifdef _WIN32
    int64_t time = convertTmToTime(tm);
#else
    int64_t time = timegm(&tm);
#endif
    if (time == -1) {
        return KuzuError;
    }
    out_result->days = time / 86400;
    return KuzuSuccess;
}

kuzu_state kuzu_date_to_string(kuzu_date_t date, char** out_result) {
    struct tm tm;
    if (kuzu_date_to_tm(date, &tm) != KuzuSuccess) {
        return KuzuError;
    }
    char buffer[80];
    if (strftime(buffer, 80, "%Y-%m-%d", &tm) == 0) {
        return KuzuError;
    }
    *out_result = convertToOwnedCString(buffer);
    return KuzuSuccess;
}

kuzu_state kuzu_date_from_string(const char* str, kuzu_date_t* out_result) {
    try {
        date_t date = Date::fromCString(str, strlen(str));
        out_result->days = date.days;
    } catch (ConversionException& e) {
        return KuzuError;
    }
    return KuzuSuccess;
}

void kuzu_interval_to_difftime(kuzu_interval_t interval, double* out_result) {
    auto micros = interval.micros + interval.months * Interval::MICROS_PER_MONTH +
                  interval.days * Interval::MICROS_PER_DAY;
    double seconds = micros / 1000000.0;
    *out_result = seconds;
}

void kuzu_interval_from_difftime(double difftime, kuzu_interval_t* out_result) {
    int64_t total_micros = static_cast<int64_t>(difftime * 1000000);
    out_result->months = total_micros / Interval::MICROS_PER_MONTH;
    total_micros -= out_result->months * Interval::MICROS_PER_MONTH;
    out_result->days = total_micros / Interval::MICROS_PER_DAY;
    total_micros -= out_result->days * Interval::MICROS_PER_DAY;
    out_result->micros = total_micros;
}
