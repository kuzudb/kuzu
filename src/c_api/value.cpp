#include "common/types/value.h"

#include "c_api/kuzu.h"
#include "common/types/internal_id_t.h"
#include "common/types/types.h"
#include "main/kuzu.h"

using namespace kuzu::common;
using namespace kuzu::main;

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

kuzu_value* kuzu_value_create_node_val(kuzu_node_val* val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    auto node_val = std::make_unique<NodeVal>(*static_cast<NodeVal*>(val_->_node_val));
    c_value->_value = new Value(std::move(node_val));
    return c_value;
}

kuzu_value* kuzu_value_create_rel_val(kuzu_rel_val* val_) {
    auto* c_value = (kuzu_value*)calloc(1, sizeof(kuzu_value));
    auto rel_val = std::make_unique<RelVal>(*static_cast<RelVal*>(val_->_rel_val));
    c_value->_value = new Value(std::move(rel_val));
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
    auto& list_val = static_cast<Value*>(value->_value)->getListValReference();
    return list_val.size();
}

kuzu_value* kuzu_value_get_list_element(kuzu_value* value, uint64_t index) {
    auto& list_val = static_cast<Value*>(value->_value)->getListValReference();
    if (index >= list_val.size()) {
        return nullptr;
    }
    auto& list_element = list_val[index];
    auto val = list_element.get();
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_value = val;
    c_value->_is_owned_by_cpp = true;
    return c_value;
}

uint64_t kuzu_value_get_struct_num_fields(kuzu_value* value) {
    auto val = static_cast<Value*>(value->_value);
    auto data_type = val->getDataType();
    return StructType::getNumFields(&data_type);
}

char* kuzu_value_get_struct_field_name(kuzu_value* value, uint64_t index) {
    auto val = static_cast<Value*>(value->_value);
    auto data_type = val->getDataType();
    auto struct_field_name = StructType::getFields(&data_type)[index]->getName();
    auto* c_struct_field_name = (char*)malloc(sizeof(char) * (struct_field_name.size() + 1));
    strcpy(c_struct_field_name, struct_field_name.c_str());
    return c_struct_field_name;
}

kuzu_value* kuzu_value_get_struct_field_value(kuzu_value* value, uint64_t index) {
    return kuzu_value_get_list_element(value, index);
}

kuzu_logical_type* kuzu_value_get_data_type(kuzu_value* value) {
    auto* c_data_type = (kuzu_logical_type*)malloc(sizeof(kuzu_logical_type));
    c_data_type->_data_type = new LogicalType(static_cast<Value*>(value->_value)->getDataType());
    return c_data_type;
}

bool kuzu_value_get_bool(kuzu_value* value) {
    return static_cast<Value*>(value->_value)->getValue<bool>();
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

kuzu_node_val* kuzu_value_get_node_val(kuzu_value* value) {
    auto node_val = static_cast<Value*>(value->_value)->getValue<NodeVal>();
    auto* c_node_val = (kuzu_node_val*)calloc(1, sizeof(kuzu_node_val));
    c_node_val->_node_val = new NodeVal(node_val);
    return c_node_val;
}

kuzu_rel_val* kuzu_value_get_rel_val(kuzu_value* value) {
    auto rel_val = static_cast<Value*>(value->_value)->getValue<RelVal>();
    auto* c_rel_val = (kuzu_rel_val*)calloc(1, sizeof(kuzu_rel_val));
    c_rel_val->_rel_val = new RelVal(rel_val);
    return c_rel_val;
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
    auto string_val = static_cast<Value*>(value->_value)->getValue<std::string>();
    auto* c_string = (char*)malloc(string_val.size() + 1);
    strcpy(c_string, string_val.c_str());
    return c_string;
}

char* kuzu_value_to_string(kuzu_value* value) {
    auto string_val = static_cast<Value*>(value->_value)->toString();
    auto* c_string = (char*)malloc(string_val.size() + 1);
    strcpy(c_string, string_val.c_str());
    return c_string;
}

kuzu_node_val* kuzu_node_val_create(kuzu_internal_id_t id, const char* label) {
    auto id_val = std::make_unique<Value>(internalID_t(id.offset, id.table_id));
    auto label_val = std::make_unique<Value>(label);
    auto* node_val = new NodeVal(std::move(id_val), std::move(label_val));
    auto* c_node_val = (kuzu_node_val*)calloc(1, sizeof(kuzu_node_val));
    c_node_val->_node_val = node_val;
    return c_node_val;
}

kuzu_node_val* kuzu_node_val_clone(kuzu_node_val* node_val) {
    auto* c_node_val = (kuzu_node_val*)calloc(1, sizeof(kuzu_node_val));
    c_node_val->_node_val = new NodeVal(*static_cast<NodeVal*>(node_val->_node_val));
    return c_node_val;
}

void kuzu_node_val_destroy(kuzu_node_val* node_val) {
    if (node_val == nullptr) {
        return;
    }
    if ((node_val->_node_val != nullptr) && (!node_val->_is_owned_by_cpp)) {
        delete static_cast<NodeVal*>(node_val->_node_val);
    }
    free(node_val);
}

kuzu_value* kuzu_node_val_get_id_val(kuzu_node_val* node_val) {
    auto id_val = static_cast<NodeVal*>(node_val->_node_val)->getNodeIDVal();
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_value = id_val;
    c_value->_is_owned_by_cpp = true;
    return c_value;
}

kuzu_value* kuzu_node_val_get_label_val(kuzu_node_val* node_val) {
    auto label_val = static_cast<NodeVal*>(node_val->_node_val)->getLabelVal();
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_value = label_val;
    c_value->_is_owned_by_cpp = true;
    return c_value;
}

kuzu_internal_id_t kuzu_node_val_get_id(kuzu_node_val* node_val) {
    auto id = static_cast<NodeVal*>(node_val->_node_val)->getNodeID();
    kuzu_internal_id_t c_id;
    c_id.offset = id.offset;
    c_id.table_id = id.tableID;
    return c_id;
}

char* kuzu_node_val_get_label_name(kuzu_node_val* node_val) {
    auto label = static_cast<NodeVal*>(node_val->_node_val)->getLabelName();
    auto* c_string = (char*)malloc(label.size() + 1);
    strcpy(c_string, label.c_str());
    return c_string;
}

uint64_t kuzu_node_val_get_property_size(kuzu_node_val* node_val) {
    return static_cast<NodeVal*>(node_val->_node_val)->getProperties().size();
}

char* kuzu_node_val_get_property_name_at(kuzu_node_val* node_val, uint64_t index) {
    auto name = static_cast<NodeVal*>(node_val->_node_val)->getProperties().at(index).first;
    auto* c_string = (char*)malloc(name.size() + 1);
    strcpy(c_string, name.c_str());
    return c_string;
}

kuzu_value* kuzu_node_val_get_property_value_at(kuzu_node_val* node_val, uint64_t index) {
    auto& value = static_cast<NodeVal*>(node_val->_node_val)->getProperties().at(index).second;
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_value = value.get();
    c_value->_is_owned_by_cpp = true;
    return c_value;
}

void kuzu_node_val_add_property(kuzu_node_val* node_val, const char* name, kuzu_value* property) {
    auto value_ = std::make_unique<Value>(*static_cast<Value*>(property->_value));
    static_cast<NodeVal*>(node_val->_node_val)->addProperty(std::string(name), std::move(value_));
}

char* kuzu_node_val_to_string(kuzu_node_val* node_val) {
    auto string_val = static_cast<NodeVal*>(node_val->_node_val)->toString();
    auto* c_string = (char*)malloc(string_val.size() + 1);
    strcpy(c_string, string_val.c_str());
    return c_string;
}

kuzu_rel_val* kuzu_rel_val_create(
    kuzu_internal_id_t src_id, kuzu_internal_id_t dst_id, const char* label) {
    auto src_id_val = std::make_unique<Value>(internalID_t(src_id.offset, src_id.table_id));
    auto dst_id_val = std::make_unique<Value>(internalID_t(dst_id.offset, dst_id.table_id));
    auto label_val =
        std::make_unique<Value>(LogicalType{LogicalTypeID::STRING}, std::string(label));
    auto* c_rel_val = (kuzu_rel_val*)calloc(1, sizeof(kuzu_rel_val));
    c_rel_val->_rel_val =
        new RelVal(std::move(src_id_val), std::move(dst_id_val), std::move(label_val));
    return c_rel_val;
}

kuzu_rel_val* kuzu_rel_val_clone(kuzu_rel_val* rel_val) {
    auto* c_rel_val = (kuzu_rel_val*)calloc(1, sizeof(kuzu_rel_val));
    c_rel_val->_rel_val = new RelVal(*static_cast<RelVal*>(rel_val->_rel_val));
    return c_rel_val;
}

void kuzu_rel_val_destroy(kuzu_rel_val* rel_val) {
    if (rel_val == nullptr) {
        return;
    }
    if ((rel_val->_rel_val != nullptr) && (!rel_val->_is_owned_by_cpp)) {
        delete static_cast<RelVal*>(rel_val->_rel_val);
    }
    free(rel_val);
}

kuzu_value* kuzu_rel_val_get_src_id_val(kuzu_rel_val* rel_val) {
    auto src_id_val = static_cast<RelVal*>(rel_val->_rel_val)->getSrcNodeIDVal();
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_value = src_id_val;
    c_value->_is_owned_by_cpp = true;
    return c_value;
}

kuzu_value* kuzu_rel_val_get_dst_id_val(kuzu_rel_val* rel_val) {
    auto dst_id_val = static_cast<RelVal*>(rel_val->_rel_val)->getDstNodeIDVal();
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_value = dst_id_val;
    c_value->_is_owned_by_cpp = true;
    return c_value;
}

kuzu_internal_id_t kuzu_rel_val_get_src_id(kuzu_rel_val* rel_val) {
    auto id = static_cast<RelVal*>(rel_val->_rel_val)->getSrcNodeID();
    kuzu_internal_id_t c_id;
    c_id.offset = id.offset;
    c_id.table_id = id.tableID;
    return c_id;
}

kuzu_internal_id_t kuzu_rel_val_get_dst_id(kuzu_rel_val* rel_val) {
    auto id = static_cast<RelVal*>(rel_val->_rel_val)->getDstNodeID();
    kuzu_internal_id_t c_id;
    c_id.offset = id.offset;
    c_id.table_id = id.tableID;
    return c_id;
}

char* kuzu_rel_val_get_label_name(kuzu_rel_val* rel_val) {
    auto label = static_cast<RelVal*>(rel_val->_rel_val)->getLabelName();
    auto* c_string = (char*)malloc(label.size() + 1);
    strcpy(c_string, label.c_str());
    return c_string;
}

uint64_t kuzu_rel_val_get_property_size(kuzu_rel_val* rel_val) {
    return static_cast<RelVal*>(rel_val->_rel_val)->getProperties().size();
}
char* kuzu_rel_val_get_property_name_at(kuzu_rel_val* rel_val, uint64_t index) {
    auto& name = static_cast<RelVal*>(rel_val->_rel_val)->getProperties().at(index).first;
    auto* c_string = (char*)malloc(name.size() + 1);
    strcpy(c_string, name.c_str());
    return c_string;
}

kuzu_value* kuzu_rel_val_get_property_value_at(kuzu_rel_val* rel_val, uint64_t index) {
    auto& value = static_cast<RelVal*>(rel_val->_rel_val)->getProperties().at(index).second;
    auto* c_value = (kuzu_value*)malloc(sizeof(kuzu_value));
    c_value->_value = value.get();
    c_value->_is_owned_by_cpp = true;
    return c_value;
}

void kuzu_rel_val_add_property(kuzu_rel_val* rel_val, const char* name, kuzu_value* property) {
    auto value_ = std::make_unique<Value>(*static_cast<Value*>(property->_value));
    static_cast<RelVal*>(rel_val->_rel_val)->addProperty(std::string(name), std::move(value_));
}

char* kuzu_rel_val_to_string(kuzu_rel_val* rel_val) {
    auto string_val = static_cast<RelVal*>(rel_val->_rel_val)->toString();
    auto* c_string = (char*)malloc(string_val.size() + 1);
    strcpy(c_string, string_val.c_str());
    return c_string;
}
