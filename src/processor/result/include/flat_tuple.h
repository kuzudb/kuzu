#pragma once

#include <memory>

#include "src/common/include/exception.h"
#include "src/common/include/type_utils.h"
#include "src/common/types/include/value.h"

using namespace kuzu::common;

namespace kuzu {
namespace processor {

class ResultValue {

public:
    explicit ResultValue(DataType dataType) : dataType{dataType}, isNull{false} {}

    inline void setNull(bool isNull) { this->isNull = isNull; }

    inline bool getBooleanVal() const { return val.booleanVal; }

    inline int64_t getInt64Val() const { return val.int64Val; }

    inline double getDoubleVal() const { return val.doubleVal; }

    inline date_t getDateVal() const { return val.dateVal; }

    inline timestamp_t getTimestampVal() const { return val.timestampVal; }

    inline interval_t getIntervalVal() const { return val.intervalVal; }

    inline string getStringVal() const { return stringVal; }

    inline vector<ResultValue> getListVal() const { return listVal; }

    inline bool isNullVal() const { return isNull; }

    inline DataType getDataType() const { return dataType; }

    void set(const uint8_t* value, DataType& valueType);

    string toString() const;

private:
    vector<ResultValue> convertKUListToVector(ku_list_t& list) const;

    void setFromUnstructuredValue(Value& value);

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
    // Note: dataType cannot be UNSTRUCTURED. Any Value has a fixed known data type.
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
