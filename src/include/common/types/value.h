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
    /**
     * @return a NULL value of ANY type.
     */
    KUZU_API static Value createNullValue();
    /**
     * @param dataType the type of the NULL value.
     * @return a NULL value of the given type.
     */
    KUZU_API static Value createNullValue(LogicalType dataType);
    /**
     * @param dataType the type of the non-NULL value.
     * @return a default non-NULL value of the given type.
     */
    KUZU_API static Value createDefaultValue(const LogicalType& dataType);
    /**
     * @param val_ the boolean value to set.
     * @return a Value with BOOL type and val_ value.
     */
    KUZU_API explicit Value(bool val_);
    /**
     * @param val_ the int16_t value to set.
     * @return a Value with INT16 type and val_ value.
     */
    KUZU_API explicit Value(int16_t val_);
    /**
     * @param val_ the int32_t value to set.
     * @return a Value with INT32 type and val_ value.
     */
    KUZU_API explicit Value(int32_t val_);
    /**
     * @param val_ the int64_t value to set.
     * @return a Value with INT64 type and val_ value.
     */
    KUZU_API explicit Value(int64_t val_);
    /**
     * @param val_ the double value to set.
     * @return a Value with DOUBLE type and val_ value.
     */
    KUZU_API explicit Value(double val_);
    /**
     * @param val_ the float value to set.
     * @return a Value with FLOAT type and val_ value.
     */
    KUZU_API explicit Value(float_t val_);
    /**
     * @param val_ the date value to set.
     * @return a Value with DATE type and val_ value.
     */
    KUZU_API explicit Value(date_t val_);
    /**
     * @param val_ the timestamp value to set.
     * @return a Value with TIMESTAMP type and val_ value.
     */
    KUZU_API explicit Value(timestamp_t val_);
    /**
     * @param val_ the interval value to set.
     * @return a Value with INTERVAL type and val_ value.
     */
    KUZU_API explicit Value(interval_t val_);
    /**
     * @param val_ the internalID value to set.
     * @return a Value with INTERNAL_ID type and val_ value.
     */
    KUZU_API explicit Value(internalID_t val_);
    /**
     * @param val_ the string value to set.
     * @return a Value with STRING type and val_ value.
     */
    KUZU_API explicit Value(const char* val_);
    /**
     * @param val_ the string value to set.
     * @return a Value with type and val_ value.
     */
    KUZU_API explicit Value(LogicalType type, const std::string& val_);
    /**
     * @param vals the list value to set.
     * @return a Value with dataType type and vals value.
     */
    KUZU_API explicit Value(LogicalType dataType, std::vector<std::unique_ptr<Value>> vals);
    /**
     * @param val_ the node value to set.
     * @return a Value with NODE type and val_ value.
     */
    KUZU_API explicit Value(std::unique_ptr<NodeVal> val_);
    /**
     * @param val_ the rel value to set.
     * @return a Value with REL type and val_ value.
     */
    KUZU_API explicit Value(std::unique_ptr<RelVal> val_);
    /**
     * @param val_ the value to set.
     * @return a Value with dataType type and val_ value.
     */
    KUZU_API explicit Value(LogicalType dataType, const uint8_t* val_);
    /**
     * @param other the value to copy from.
     * @return a Value with the same value as other.
     */
    KUZU_API Value(const Value& other);
    /**
     * @brief Sets the data type of the Value.
     * @param dataType_ the data type to set to.
     */
    KUZU_API void setDataType(const LogicalType& dataType_);
    /**
     * @return the dataType of the value.
     */
    KUZU_API LogicalType getDataType() const;
    /**
     * @brief Sets the null flag of the Value.
     * @param flag null value flag to set.
     */
    KUZU_API void setNull(bool flag);
    /**
     * @brief Sets the null flag of the Value to true.
     */
    KUZU_API void setNull();
    /**
     * @return whether the Value is null or not.
     */
    KUZU_API bool isNull() const;
    /**
     * @brief Copies from the value.
     * @param value value to copy from.
     */
    KUZU_API void copyValueFrom(const uint8_t* value);
    /**
     * @brief Copies from the other.
     * @param other value to copy from.
     */
    KUZU_API void copyValueFrom(const Value& other);
    /**
     * @return the value of the given type.
     */
    KUZU_API template<class T>
    T getValue() const {
        throw std::runtime_error("Unimplemented template for Value::getValue()");
    }
    /**
     * @return a reference to the value of the given type.
     */
    KUZU_API template<class T>
    T& getValueReference() {
        throw std::runtime_error("Unimplemented template for Value::getValueReference()");
    }
    /**
     * @return a reference to the list value.
     */
    // TODO(Guodong): think how can we template list get functions.
    KUZU_API const std::vector<std::unique_ptr<Value>>& getListValReference() const;
    /**
     * @param value the value to Value object.
     * @return a Value object based on value.
     */
    KUZU_API template<class T>
    static Value createValue(T value) {
        throw std::runtime_error("Unimplemented template for Value::createValue()");
    }
    /**
     * @return a copy of the current value.
     */
    KUZU_API std::unique_ptr<Value> copy() const;
    /**
     * @return the current value in string format.
     */
    KUZU_API std::string toString() const;

private:
    Value();
    explicit Value(LogicalType dataType);

    template<typename T>
    static inline void putValuesIntoVector(std::vector<std::unique_ptr<Value>>& fixedListResultVal,
        const uint8_t* fixedList, uint64_t numBytesPerElement) {
        for (auto i = 0; i < fixedListResultVal.size(); ++i) {
            fixedListResultVal[i] =
                std::make_unique<Value>(*(T*)(fixedList + i * numBytesPerElement));
        }
    }

    std::vector<std::unique_ptr<Value>> convertKUVarListToVector(
        ku_list_t& list, const LogicalType& childType) const;
    std::vector<std::unique_ptr<Value>> convertKUFixedListToVector(const uint8_t* fixedList) const;
    std::vector<std::unique_ptr<Value>> convertKUStructToVector(const uint8_t* kuStruct) const;
    std::vector<std::unique_ptr<Value>> convertKUUnionToVector(const uint8_t* kuUnion) const;

public:
    LogicalType dataType;
    bool isNull_;

    union Val {
        constexpr Val() : booleanVal{false} {}
        bool booleanVal;
        int64_t int64Val;
        int32_t int32Val;
        int16_t int16Val;
        double doubleVal;
        float floatVal;
        interval_t intervalVal;
        internalID_t internalIDVal;
    } val;
    std::string strVal;
    std::vector<std::unique_ptr<Value>> nestedTypeVal;
    // TODO(Ziyi): remove these two fields once we implemented node/rel using struct.
    std::unique_ptr<NodeVal> nodeVal;
    std::unique_ptr<RelVal> relVal;
};

/**
 * @brief NodeVal represents a node in the graph and stores the nodeID, label and properties of that
 * node.
 */
class NodeVal {
public:
    /**
     * @brief Constructs the NodeVal object with the given idVal and labelVal.
     * @param idVal the nodeID value.
     * @param labelVal the name of the node.
     */
    KUZU_API NodeVal(std::unique_ptr<Value> idVal, std::unique_ptr<Value> labelVal);
    /**
     * @brief Constructs the NodeVal object from the other.
     * @param other the NodeVal to copy from.
     */
    KUZU_API NodeVal(const NodeVal& other);
    /**
     * @brief Adds a property with the given {key,value} pair to the NodeVal.
     * @param key the name of the property.
     * @param value the value of the property.
     */
    KUZU_API void addProperty(const std::string& key, std::unique_ptr<Value> value);
    /**
     * @return all properties of the NodeVal.
     */
    KUZU_API const std::vector<std::pair<std::string, std::unique_ptr<Value>>>&
    getProperties() const;
    /**
     * @return the nodeID as a Value.
     */
    KUZU_API Value* getNodeIDVal();
    /**
     * @return the name of the node as a Value.
     */
    KUZU_API Value* getLabelVal();
    /**
     * @return the nodeID of the node as a nodeID struct.
     */
    KUZU_API nodeID_t getNodeID() const;
    /**
     * @return the name of the node in string format.
     */
    KUZU_API std::string getLabelName() const;
    /**
     * @return a copy of the current node.
     */
    KUZU_API std::unique_ptr<NodeVal> copy() const;
    /**
     * @return the current node values in string format.
     */
    KUZU_API std::string toString() const;

private:
    std::unique_ptr<Value> idVal;
    std::unique_ptr<Value> labelVal;
    std::vector<std::pair<std::string, std::unique_ptr<Value>>> properties;
};

/**
 * @brief RelVal represents a rel in the graph and stores the relID, src/dst nodes and properties of
 * that rel.
 */
class RelVal {
public:
    /**
     * @brief Constructs the RelVal based on the srcNodeIDVal, dstNodeIDVal and labelVal.
     * @param srcNodeIDVal the src node.
     * @param dstNodeIDVal the dst node.
     * @param labelVal the name of the rel.
     */
    KUZU_API RelVal(std::unique_ptr<Value> srcNodeIDVal, std::unique_ptr<Value> dstNodeIDVal,
        std::unique_ptr<Value> labelVal);
    /**
     * @brief Constructs a RelVal from other.
     * @param other the RelVal to copy from.
     */
    KUZU_API RelVal(const RelVal& other);
    /**
     * @brief Adds a property with the given {key,value} pair to the RelVal.
     * @param key the name of the property.
     * @param value the value of the property.
     */
    KUZU_API void addProperty(const std::string& key, std::unique_ptr<Value> value);
    /**
     * @return all properties of the RelVal.
     */
    KUZU_API const std::vector<std::pair<std::string, std::unique_ptr<Value>>>&
    getProperties() const;
    /**
     * @return the src nodeID value of the RelVal in Value.
     */
    KUZU_API Value* getSrcNodeIDVal();
    /**
     * @return the dst nodeID value of the RelVal in Value.
     */
    KUZU_API Value* getDstNodeIDVal();
    /**
     * @return the src nodeID value of the RelVal as nodeID struct.
     */
    KUZU_API nodeID_t getSrcNodeID() const;
    /**
     * @return the dst nodeID value of the RelVal as nodeID struct.
     */
    KUZU_API nodeID_t getDstNodeID() const;
    /**
     * @return the name of the RelVal.
     */
    KUZU_API std::string getLabelName() const;
    /**
     * @return the value of the RelVal in string format.
     */
    KUZU_API std::string toString() const;
    /**
     * @return a copy of the RelVal.
     */
    KUZU_API inline std::unique_ptr<RelVal> copy() const;

private:
    std::unique_ptr<Value> labelVal;
    std::unique_ptr<Value> srcNodeIDVal;
    std::unique_ptr<Value> dstNodeIDVal;
    std::vector<std::pair<std::string, std::unique_ptr<Value>>> properties;
};

/**
 * @return boolean value.
 */
KUZU_API template<>
inline bool Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
    return val.booleanVal;
}

/**
 * @return int16 value.
 */
KUZU_API template<>
inline int16_t Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INT16);
    return val.int16Val;
}

/**
 * @return int32 value.
 */
KUZU_API template<>
inline int32_t Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INT32);
    return val.int32Val;
}

/**
 * @return int64 value.
 */
KUZU_API template<>
inline int64_t Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INT64);
    return val.int64Val;
}

/**
 * @return float value.
 */
KUZU_API template<>
inline float Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::FLOAT);
    return val.floatVal;
}

/**
 * @return double value.
 */
KUZU_API template<>
inline double Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::DOUBLE);
    return val.doubleVal;
}

/**
 * @return date_t value.
 */
KUZU_API template<>
inline date_t Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::DATE);
    return date_t{val.int32Val};
}

/**
 * @return timestamp_t value.
 */
KUZU_API template<>
inline timestamp_t Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::TIMESTAMP);
    return timestamp_t{val.int64Val};
}

/**
 * @return interval_t value.
 */
KUZU_API template<>
inline interval_t Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INTERVAL);
    return val.intervalVal;
}

/**
 * @return internal_t value.
 */
KUZU_API template<>
inline internalID_t Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID);
    return val.internalIDVal;
}

/**
 * @return string value.
 */
KUZU_API template<>
inline std::string Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::STRING);
    return strVal;
}

/**
 * @return NodeVal value.
 */
KUZU_API template<>
inline NodeVal Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::NODE);
    return *nodeVal;
}

/**
 * @return RelVal value.
 */
KUZU_API template<>
inline RelVal Value::getValue() const {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::REL);
    return *relVal;
}

/**
 * @return the reference to the boolean value.
 */
KUZU_API template<>
inline bool& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::BOOL);
    return val.booleanVal;
}

/**
 * @return the reference to the int16 value.
 */
KUZU_API template<>
inline int16_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INT16);
    return val.int16Val;
}

/**
 * @return the reference to the int32 value.
 */
KUZU_API template<>
inline int32_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INT32);
    return val.int32Val;
}

/**
 * @return the reference to the int64 value.
 */
KUZU_API template<>
inline int64_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INT64);
    return val.int64Val;
}

/**
 * @return the reference to the float value.
 */
KUZU_API template<>
inline float_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::FLOAT);
    return val.floatVal;
}

/**
 * @return the reference to the double value.
 */
KUZU_API template<>
inline double_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::DOUBLE);
    return val.doubleVal;
}

/**
 * @return the reference to the date value.
 */
KUZU_API template<>
inline date_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::DATE);
    return *reinterpret_cast<date_t*>(&val.int32Val);
}

/**
 * @return the reference to the timestamp value.
 */
KUZU_API template<>
inline timestamp_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::TIMESTAMP);
    return *reinterpret_cast<timestamp_t*>(&val.int64Val);
}

/**
 * @return the reference to the interval value.
 */
KUZU_API template<>
inline interval_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INTERVAL);
    return val.intervalVal;
}

/**
 * @return the reference to the internal_id value.
 */
KUZU_API template<>
inline nodeID_t& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::INTERNAL_ID);
    return val.internalIDVal;
}

/**
 * @return the reference to the string value.
 */
KUZU_API template<>
inline std::string& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::STRING);
    return strVal;
}

/**
 * @return the reference to the NodeVal value.
 */
KUZU_API template<>
inline NodeVal& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::NODE);
    return *nodeVal;
}

/**
 * @return the reference to the RelVal value.
 */
KUZU_API template<>
inline RelVal& Value::getValueReference() {
    assert(dataType.getLogicalTypeID() == LogicalTypeID::REL);
    return *relVal;
}

/**
 * @param val the boolean value
 * @return a Value with BOOL type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(bool val) {
    return Value(val);
}

/**
 * @param val the int16 value
 * @return a Value with INT16 type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(int16_t val) {
    return Value(val);
}

/**
 * @param val the int32 value
 * @return a Value with INT32 type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(int32_t val) {
    return Value(val);
}

/**
 * @param val the int64 value
 * @return a Value with INT64 type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(int64_t val) {
    return Value(val);
}

/**
 * @param val the double value
 * @return a Value with DOUBLE type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(double val) {
    return Value(val);
}

/**
 * @param val the date_t value
 * @return a Value with DATE type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(date_t val) {
    return Value(val);
}

/**
 * @param val the timestamp_t value
 * @return a Value with TIMESTAMP type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(timestamp_t val) {
    return Value(val);
}

/**
 * @param val the interval_t value
 * @return a Value with INTERVAL type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(interval_t val) {
    return Value(val);
}

/**
 * @param val the nodeID_t value
 * @return a Value with NODE_ID type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(nodeID_t val) {
    return Value(val);
}

/**
 * @param val the string value
 * @return a Value with type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(std::string val) {
    return Value(LogicalType{LogicalTypeID::STRING}, val);
}

/**
 * @param val the string value
 * @return a Value with STRING type and val value.
 */
KUZU_API template<>
inline Value Value::createValue(const char* value) {
    return Value(LogicalType{LogicalTypeID::STRING}, std::string(value));
}

} // namespace common
} // namespace kuzu
