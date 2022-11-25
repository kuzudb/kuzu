#pragma once

#include <memory>

#include "common/exception.h"
#include "common/type_utils.h"
#include "common/utils.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

class ResultValue {

public:
    explicit ResultValue(DataType dataType) : dataType{dataType}, isNull{false} {}

    inline void setNull(bool isNull) { this->isNull = isNull; }

    inline bool getBooleanVal() const {
        errorIfTypeMismatch(BOOL);
        return val.booleanVal;
    }

    inline int64_t getInt64Val() const {
        errorIfTypeMismatch(INT64);
        return val.int64Val;
    }

    inline double getDoubleVal() const {
        errorIfTypeMismatch(DOUBLE);
        return val.doubleVal;
    }

    inline date_t getDateVal() const {
        errorIfTypeMismatch(DATE);
        return val.dateVal;
    }

    inline timestamp_t getTimestampVal() const {
        errorIfTypeMismatch(TIMESTAMP);
        return val.timestampVal;
    }

    inline interval_t getIntervalVal() const {
        errorIfTypeMismatch(INTERVAL);
        return val.intervalVal;
    }

    inline string getStringVal() const {
        errorIfTypeMismatch(STRING);
        return stringVal;
    }

    inline vector<ResultValue> getListVal() const {
        errorIfTypeMismatch(LIST);
        return listVal;
    }

    inline bool isNullVal() const { return isNull; }

    inline DataType getDataType() const { return dataType; }

    void set(const uint8_t* value, DataType& valueType);

    string toString() const;

private:
    inline void errorIfTypeMismatch(DataTypeID typeID) const {
        if (typeID != dataType.typeID) {
            throw RuntimeException(
                StringUtils::string_format("Cannot get %s value from the %s result value.",
                    Types::dataTypeToString(typeID).c_str(),
                    Types::dataTypeToString(dataType.typeID).c_str()));
        }
    }

    vector<ResultValue> convertKUListToVector(ku_list_t& list) const;

private:
    union Val {
        constexpr Val() : booleanVal{false} {}
        bool booleanVal;
        int64_t int64Val;
        double doubleVal;
        date_t dateVal;
        timestamp_t timestampVal;
        interval_t intervalVal;
    } val;
    string stringVal;
    vector<ResultValue> listVal;
    DataType dataType;
    bool isNull;
};

class FlatTuple {

public:
    explicit FlatTuple(const vector<DataType>& valueTypes) {
        resultValues.resize(valueTypes.size());
        for (auto i = 0u; i < valueTypes.size(); i++) {
            resultValues[i] = make_unique<ResultValue>(valueTypes[i]);
        }
    }

    inline uint32_t len() { return resultValues.size(); }

    ResultValue* getResultValue(uint32_t valIdx);

    string toString(const vector<uint32_t>& colsWidth, const string& delimiter = "|");

private:
    vector<unique_ptr<ResultValue>> resultValues;
};

} // namespace processor
} // namespace kuzu
