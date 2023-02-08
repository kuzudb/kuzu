#pragma once

#include "common/api.h"
#include "common/exception.h"
#include "common/type_utils.h"
#include "common/utils.h"

namespace kuzu {
namespace common {

class NodeVal;
class RelVal;

class Value {
public:
    // Create a NULL value of any type.
    KUZU_API static Value createNullValue();
    // Create a NULL value of a given type.
    KUZU_API static Value createNullValue(DataType dataType);
    // Create a default non-NULL value of a given type
    KUZU_API static Value createDefaultValue(const DataType& dataType);

    KUZU_API explicit Value(bool val_);
    KUZU_API explicit Value(int32_t val_);
    KUZU_API explicit Value(int64_t val_);
    KUZU_API explicit Value(double val_);
    KUZU_API explicit Value(date_t val_);
    KUZU_API explicit Value(timestamp_t val_);
    KUZU_API explicit Value(interval_t val_);
    KUZU_API explicit Value(internalID_t val_);
    KUZU_API explicit Value(const char* val_);
    KUZU_API explicit Value(const std::string& val_);
    KUZU_API explicit Value(DataType dataType, std::vector<std::unique_ptr<Value>> vals);
    KUZU_API explicit Value(std::unique_ptr<NodeVal> val_);
    KUZU_API explicit Value(std::unique_ptr<RelVal> val_);
    KUZU_API explicit Value(DataType dataType, const uint8_t* val_);

    KUZU_API Value(const Value& other);

    KUZU_API void setDataType(const DataType& dataType_);
    KUZU_API DataType getDataType() const;

    KUZU_API void setNull(bool flag);
    KUZU_API void setNull();
    KUZU_API bool isNull() const;

    KUZU_API void copyValueFrom(const uint8_t* value);
    KUZU_API void copyValueFrom(const Value& other);

    KUZU_API template<class T>
    T getValue() const {
        throw std::runtime_error("Unimplemented template for Value::getValue()");
    }

    KUZU_API template<class T>
    T& getValueReference() {
        throw std::runtime_error("Unimplemented template for Value::getValueReference()");
    }
    // TODO(Guodong): think how can we template list get functions.
    KUZU_API const std::vector<std::unique_ptr<Value>>& getListValReference() const;

    KUZU_API template<class T>
    static Value createValue(T value) {
        throw std::runtime_error("Unimplemented template for Value::createValue()");
    }

    KUZU_API std::unique_ptr<Value> copy() const;

    KUZU_API std::string toString() const;

private:
    Value();
    explicit Value(DataType dataType);

    void validateType(DataTypeID typeID) const;
    void validateType(const DataType& type) const;

    std::vector<std::unique_ptr<Value>> convertKUListToVector(ku_list_t& list) const;

public:
    DataType dataType;
    bool isNull_;

    union Val {
        constexpr Val() : booleanVal{false} {}
        bool booleanVal;
        int64_t int64Val;
        double doubleVal;
        date_t dateVal;
        timestamp_t timestampVal;
        interval_t intervalVal;
        internalID_t internalIDVal;
    } val;
    std::string strVal;
    std::vector<std::unique_ptr<Value>> listVal;
    std::unique_ptr<NodeVal> nodeVal;
    std::unique_ptr<RelVal> relVal;
};

class NodeVal {
public:
    KUZU_API NodeVal(std::unique_ptr<Value> idVal, std::unique_ptr<Value> labelVal);

    KUZU_API NodeVal(const NodeVal& other);

    KUZU_API void addProperty(const std::string& key, std::unique_ptr<Value> value);

    KUZU_API const std::vector<std::pair<std::string, std::unique_ptr<Value>>>&
    getProperties() const;

    KUZU_API Value* getNodeIDVal();
    KUZU_API Value* getLabelVal();

    KUZU_API nodeID_t getNodeID() const;
    KUZU_API std::string getLabelName() const;

    KUZU_API std::unique_ptr<NodeVal> copy() const;

    KUZU_API std::string toString() const;

private:
    std::unique_ptr<Value> idVal;
    std::unique_ptr<Value> labelVal;
    std::vector<std::pair<std::string, std::unique_ptr<Value>>> properties;
};

class RelVal {
public:
    KUZU_API RelVal(std::unique_ptr<Value> srcNodeIDVal, std::unique_ptr<Value> dstNodeIDVal,
        std::unique_ptr<Value> labelVal);

    KUZU_API RelVal(const RelVal& other);

    KUZU_API void addProperty(const std::string& key, std::unique_ptr<Value> value);

    KUZU_API const std::vector<std::pair<std::string, std::unique_ptr<Value>>>&
    getProperties() const;

    KUZU_API Value* getSrcNodeIDVal();
    KUZU_API Value* getDstNodeIDVal();

    KUZU_API nodeID_t getSrcNodeID() const;
    KUZU_API nodeID_t getDstNodeID() const;
    KUZU_API std::string getLabelName();

    KUZU_API std::string toString() const;

    KUZU_API inline std::unique_ptr<RelVal> copy() const;

private:
    std::unique_ptr<Value> labelVal;
    std::unique_ptr<Value> srcNodeIDVal;
    std::unique_ptr<Value> dstNodeIDVal;
    std::vector<std::pair<std::string, std::unique_ptr<Value>>> properties;
};

KUZU_API template<>
inline bool Value::getValue() const {
    assert(dataType.getTypeID() == BOOL);
    return val.booleanVal;
}

KUZU_API template<>
inline int64_t Value::getValue() const {
    assert(dataType.getTypeID() == INT64);
    return val.int64Val;
}

KUZU_API template<>
inline double Value::getValue() const {
    assert(dataType.getTypeID() == DOUBLE);
    return val.doubleVal;
}

KUZU_API template<>
inline date_t Value::getValue() const {
    assert(dataType.getTypeID() == DATE);
    return val.dateVal;
}

KUZU_API template<>
inline timestamp_t Value::getValue() const {
    assert(dataType.getTypeID() == TIMESTAMP);
    return val.timestampVal;
}

KUZU_API template<>
inline interval_t Value::getValue() const {
    assert(dataType.getTypeID() == INTERVAL);
    return val.intervalVal;
}

KUZU_API template<>
inline internalID_t Value::getValue() const {
    assert(dataType.getTypeID() == INTERNAL_ID);
    return val.internalIDVal;
}

KUZU_API template<>
inline std::string Value::getValue() const {
    assert(dataType.getTypeID() == STRING);
    return strVal;
}

KUZU_API template<>
inline NodeVal Value::getValue() const {
    assert(dataType.getTypeID() == NODE);
    return *nodeVal;
}

KUZU_API template<>
inline RelVal Value::getValue() const {
    assert(dataType.getTypeID() == REL);
    return *relVal;
}

KUZU_API template<>
inline bool& Value::getValueReference() {
    assert(dataType.getTypeID() == BOOL);
    return val.booleanVal;
}

KUZU_API template<>
inline int64_t& Value::getValueReference() {
    assert(dataType.getTypeID() == INT64);
    return val.int64Val;
}

KUZU_API template<>
inline double& Value::getValueReference() {
    assert(dataType.getTypeID() == DOUBLE);
    return val.doubleVal;
}

KUZU_API template<>
inline date_t& Value::getValueReference() {
    assert(dataType.getTypeID() == DATE);
    return val.dateVal;
}

KUZU_API template<>
inline timestamp_t& Value::getValueReference() {
    assert(dataType.getTypeID() == TIMESTAMP);
    return val.timestampVal;
}

KUZU_API template<>
inline interval_t& Value::getValueReference() {
    assert(dataType.getTypeID() == INTERVAL);
    return val.intervalVal;
}

KUZU_API template<>
inline nodeID_t& Value::getValueReference() {
    assert(dataType.getTypeID() == INTERNAL_ID);
    return val.internalIDVal;
}

KUZU_API template<>
inline std::string& Value::getValueReference() {
    assert(dataType.getTypeID() == STRING);
    return strVal;
}

KUZU_API template<>
inline NodeVal& Value::getValueReference() {
    assert(dataType.getTypeID() == NODE);
    return *nodeVal;
}

KUZU_API template<>
inline RelVal& Value::getValueReference() {
    assert(dataType.getTypeID() == REL);
    return *relVal;
}

KUZU_API template<>
inline Value Value::createValue(bool val) {
    return Value(val);
}

KUZU_API template<>
inline Value Value::createValue(int64_t val) {
    return Value(val);
}

KUZU_API template<>
inline Value Value::createValue(double val) {
    return Value(val);
}

KUZU_API template<>
inline Value Value::createValue(date_t val) {
    return Value(val);
}

KUZU_API template<>
inline Value Value::createValue(timestamp_t val) {
    return Value(val);
}

KUZU_API template<>
inline Value Value::createValue(interval_t val) {
    return Value(val);
}

KUZU_API template<>
inline Value Value::createValue(nodeID_t val) {
    return Value(val);
}

KUZU_API template<>
inline Value Value::createValue(std::string val) {
    return Value(val);
}

KUZU_API template<>
inline Value Value::createValue(const std::string& val) {
    return Value(val);
}

KUZU_API template<>
inline Value Value::createValue(const char* value) {
    return Value(std::string(value));
}

} // namespace common
} // namespace kuzu
