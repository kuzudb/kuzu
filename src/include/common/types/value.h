#pragma once

#include "../exception.h"
#include "../type_utils.h"
#include "../utils.h"

namespace kuzu {
namespace common {

class NodeVal;
class RelVal;

class Value {
public:
    // Create a NULL value of any type.
    static Value createNullValue();
    // Create a NULL value of a given type.
    static Value createNullValue(DataType dataType);
    // Create a default non-NULL value of a given type
    static Value createDefaultValue(const DataType& dataType);

    explicit Value(bool val_);
    explicit Value(int32_t val_);
    explicit Value(int64_t val_);
    explicit Value(double val_);
    explicit Value(date_t val_);
    explicit Value(timestamp_t val_);
    explicit Value(interval_t val_);
    explicit Value(internalID_t val_);
    explicit Value(const char* val_);
    explicit Value(const string& val_);
    explicit Value(DataType dataType, vector<unique_ptr<Value>> vals);
    explicit Value(unique_ptr<NodeVal> val_);
    explicit Value(unique_ptr<RelVal> val_);
    explicit Value(DataType dataType, const uint8_t* val_);

    Value(const Value& other);

    inline void setDataType(const DataType& dataType_) {
        assert(dataType.typeID == ANY);
        dataType = dataType_;
    }
    inline DataType getDataType() const { return dataType; }

    inline void setNull(bool flag) { isNull_ = flag; }
    inline void setNull() { isNull_ = true; }
    inline bool isNull() const { return isNull_; }

    void copyValueFrom(const uint8_t* value);
    void copyValueFrom(const Value& other);

    template<class T>
    T getValue() const {
        throw InternalException("Unimplemented template for Value::getValue()");
    }

    template<class T>
    T& getValueReference() {
        throw InternalException("Unimplemented template for Value::getValueReference()");
    }
    // TODO(Guodong): think how can we template list get functions.
    const vector<unique_ptr<Value>>& getListValReference() const { return listVal; }

    template<class T>
    static Value createValue(T value) {
        throw InternalException("Unimplemented template for Value::createValue()");
    }

    inline unique_ptr<Value> copy() const { return make_unique<Value>(*this); }

    string toString() const;

private:
    Value() : dataType{ANY}, isNull_{true} {}
    explicit Value(DataType dataType) : dataType{std::move(dataType)}, isNull_{true} {}

    inline void validateType(DataTypeID typeID) const { validateType(DataType(typeID)); }
    void validateType(const DataType& type) const;

    vector<unique_ptr<Value>> convertKUListToVector(common::ku_list_t& list) const;

public:
    common::DataType dataType;
    bool isNull_;

    union Val {
        constexpr Val() : booleanVal{false} {}
        bool booleanVal;
        int64_t int64Val;
        double doubleVal;
        common::date_t dateVal;
        common::timestamp_t timestampVal;
        common::interval_t intervalVal;
        common::internalID_t internalIDVal;
    } val;
    std::string strVal;
    vector<unique_ptr<Value>> listVal;
    unique_ptr<NodeVal> nodeVal;
    unique_ptr<RelVal> relVal;
};

class NodeVal {
public:
    NodeVal(unique_ptr<Value> idVal, unique_ptr<Value> labelVal)
        : idVal{std::move(idVal)}, labelVal{std::move(labelVal)} {}
    NodeVal(const NodeVal& other);

    inline void addProperty(const std::string& key, unique_ptr<Value> value) {
        properties.emplace_back(key, std::move(value));
    }

    inline const vector<pair<std::string, unique_ptr<Value>>>& getProperties() const {
        return properties;
    }

    inline Value* getNodeIDVal() { return idVal.get(); }
    inline Value* getLabelVal() { return labelVal.get(); }

    nodeID_t getNodeID() const;
    string getLabelName() const;

    inline unique_ptr<NodeVal> copy() const { return make_unique<NodeVal>(*this); }

    string toString() const;

private:
    unique_ptr<Value> idVal;
    unique_ptr<Value> labelVal;
    vector<pair<std::string, unique_ptr<Value>>> properties;
};

class RelVal {
public:
    RelVal(
        unique_ptr<Value> srcNodeIDVal, unique_ptr<Value> dstNodeIDVal, unique_ptr<Value> labelVal)
        : srcNodeIDVal{std::move(srcNodeIDVal)},
          dstNodeIDVal{std::move(dstNodeIDVal)}, labelVal{std::move(labelVal)} {}
    RelVal(const RelVal& other);

    inline void addProperty(const std::string& key, unique_ptr<Value> value) {
        properties.emplace_back(key, std::move(value));
    }

    inline const vector<pair<std::string, unique_ptr<Value>>>& getProperties() const {
        return properties;
    }

    inline Value* getSrcNodeIDVal() { return srcNodeIDVal.get(); }
    inline Value* getDstNodeIDVal() { return dstNodeIDVal.get(); }

    nodeID_t getSrcNodeID() const;
    nodeID_t getDstNodeID() const;
    string getLabelName();

    string toString() const;

    inline unique_ptr<RelVal> copy() const { return make_unique<RelVal>(*this); }

private:
    unique_ptr<Value> labelVal;
    unique_ptr<Value> srcNodeIDVal;
    unique_ptr<Value> dstNodeIDVal;
    vector<pair<std::string, unique_ptr<Value>>> properties;
};

template<>
inline bool Value::getValue() const {
    validateType(BOOL);
    return val.booleanVal;
}

template<>
inline int64_t Value::getValue() const {
    validateType(INT64);
    return val.int64Val;
}

template<>
inline double Value::getValue() const {
    validateType(DOUBLE);
    return val.doubleVal;
}

template<>
inline date_t Value::getValue() const {
    validateType(DATE);
    return val.dateVal;
}

template<>
inline timestamp_t Value::getValue() const {
    validateType(TIMESTAMP);
    return val.timestampVal;
}

template<>
inline interval_t Value::getValue() const {
    validateType(INTERVAL);
    return val.intervalVal;
}

template<>
inline nodeID_t Value::getValue() const {
    validateType(INTERNAL_ID);
    return val.internalIDVal;
}

template<>
inline string Value::getValue() const {
    validateType(STRING);
    return strVal;
}

template<>
inline NodeVal Value::getValue() const {
    validateType(NODE);
    return *nodeVal;
}

template<>
inline RelVal Value::getValue() const {
    validateType(REL);
    return *relVal;
}

template<>
inline bool& Value::getValueReference() {
    assert(dataType.typeID == BOOL);
    return val.booleanVal;
}

template<>
inline int64_t& Value::getValueReference() {
    assert(dataType.typeID == INT64);
    return val.int64Val;
}

template<>
inline double& Value::getValueReference() {
    assert(dataType.typeID == DOUBLE);
    return val.doubleVal;
}

template<>
inline date_t& Value::getValueReference() {
    assert(dataType.typeID == DATE);
    return val.dateVal;
}

template<>
inline timestamp_t& Value::getValueReference() {
    assert(dataType.typeID == TIMESTAMP);
    return val.timestampVal;
}

template<>
inline interval_t& Value::getValueReference() {
    assert(dataType.typeID == INTERVAL);
    return val.intervalVal;
}

template<>
inline nodeID_t& Value::getValueReference() {
    assert(dataType.typeID == INTERNAL_ID);
    return val.internalIDVal;
}

template<>
inline string& Value::getValueReference() {
    assert(dataType.typeID == STRING);
    return strVal;
}

template<>
inline NodeVal& Value::getValueReference() {
    assert(dataType.typeID == NODE);
    return *nodeVal;
}

template<>
inline RelVal& Value::getValueReference() {
    assert(dataType.typeID == REL);
    return *relVal;
}

template<>
inline Value Value::createValue(bool val) {
    return Value(val);
}

template<>
inline Value Value::createValue(int64_t val) {
    return Value(val);
}

template<>
inline Value Value::createValue(double val) {
    return Value(val);
}

template<>
inline Value Value::createValue(date_t val) {
    return Value(val);
}

template<>
inline Value Value::createValue(timestamp_t val) {
    return Value(val);
}

template<>
inline Value Value::createValue(interval_t val) {
    return Value(val);
}

template<>
inline Value Value::createValue(nodeID_t val) {
    return Value(val);
}

template<>
inline Value Value::createValue(string val) {
    return Value(val);
}

template<>
inline Value Value::createValue(const string& val) {
    return Value(val);
}

template<>
inline Value Value::createValue(const char* value) {
    return Value(string(value));
}

} // namespace common
} // namespace kuzu
