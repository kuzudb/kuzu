#pragma once

#include "common/exception.h"
#include "common/type_utils.h"
#include "common/utils.h"

namespace kuzu {
namespace processor {

class NodeVal;
class RelVal;

class Value {
public:
    explicit Value(common::DataType dataType) : dataType{std::move(dataType)}, isNull{false} {}

    inline bool getBooleanVal() const {
        validateType(common::BOOL);
        return val.booleanVal;
    }

    inline int64_t getInt64Val() const {
        validateType(common::INT64);
        return val.int64Val;
    }

    inline double getDoubleVal() const {
        validateType(common::DOUBLE);
        return val.doubleVal;
    }

    inline common::date_t getDateVal() const {
        validateType(common::DATE);
        return val.dateVal;
    }

    inline common::timestamp_t getTimestampVal() const {
        validateType(common::TIMESTAMP);
        return val.timestampVal;
    }

    inline common::interval_t getIntervalVal() const {
        validateType(common::INTERVAL);
        return val.intervalVal;
    }

    inline common::nodeID_t getNodeIDVal() const {
        validateType(common::NODE_ID);
        return val.nodeIDVal;
    }

    inline std::string getStringVal() const {
        validateType(common::STRING);
        return stringVal;
    }

    // TODO(Xiyang): Maybe we should do copy here.
    inline const vector<unique_ptr<Value>>& getListVal() const {
        validateType(common::LIST);
        return listVal;
    }

    inline void setNull(bool isNull_) { this->isNull = isNull_; }
    inline bool isNullVal() const { return isNull; }

    inline common::DataType getDataType() const { return dataType; }

    void set(const uint8_t* value);
    inline void setNodeVal(unique_ptr<NodeVal> nodeVal_) { nodeVal = std::move(nodeVal_); }
    inline void setRelVal(unique_ptr<RelVal> relVal_) { relVal = std::move(relVal_); }

    string toString() const;

private:
    void validateType(common::DataTypeID typeID) const;

    vector<unique_ptr<Value>> convertKUListToVector(common::ku_list_t& list) const;

private:
    union Val {
        constexpr Val() : booleanVal{false} {}
        bool booleanVal;
        int64_t int64Val;
        double doubleVal;
        common::date_t dateVal;
        common::timestamp_t timestampVal;
        common::interval_t intervalVal;
        common::nodeID_t nodeIDVal;
    } val;
    std::string stringVal;
    vector<unique_ptr<Value>> listVal;
    unique_ptr<NodeVal> nodeVal;
    unique_ptr<RelVal> relVal;

    common::DataType dataType;
    bool isNull;
};

class NodeOrRelVal {
public:
    inline void addProperty(const std::string& key, unique_ptr<Value> value) {
        properties.emplace_back(key, std::move(value));
    }

    virtual string toString() const = 0;

protected:
    string propertiesToString() const;

protected:
    vector<pair<std::string, unique_ptr<Value>>> properties;
};

class NodeVal : public NodeOrRelVal {
public:
    NodeVal(unique_ptr<Value> idVal, unique_ptr<Value> labelVal)
        : idVal{std::move(idVal)}, labelVal{std::move(labelVal)} {}

    inline common::nodeID_t getNodeID() const { return idVal->getNodeIDVal(); }
    inline std::string getLabelName() const { return labelVal->getStringVal(); }

    string toString() const override;

private:
    unique_ptr<Value> idVal;
    unique_ptr<Value> labelVal;
};

class RelVal : public NodeOrRelVal {
public:
    RelVal(unique_ptr<Value> srcNodeIDVal, unique_ptr<Value> dstNodeIDVal)
        : srcNodeIDVal{std::move(srcNodeIDVal)}, dstNodeIDVal{std::move(dstNodeIDVal)} {}

    string toString() const override;

private:
    unique_ptr<Value> srcNodeIDVal;
    unique_ptr<Value> dstNodeIDVal;
};

} // namespace processor
} // namespace kuzu
