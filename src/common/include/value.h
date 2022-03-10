#pragma once

#include <cstdint>
#include <string>

#include "src/common/include/gf_list.h"
#include "src/common/include/gf_string.h"
#include "src/common/include/types.h"

using namespace std;

namespace graphflow {
namespace common {

class Value {
public:
    Value() = default;

    Value(const Value& value) { *this = value; }

    explicit Value(DataType dataType) : dataType(dataType) {}

    explicit Value(bool value) : dataType(BOOL) { this->val.booleanVal = value; }

    explicit Value(int64_t value) : dataType(INT64) { this->val.int64Val = value; }

    explicit Value(double value) : dataType(DOUBLE) { this->val.doubleVal = value; }

    explicit Value(const string& value) : dataType(STRING) { this->val.strVal.set(value); }

    explicit Value(nodeID_t value) : dataType(NODE) { this->val.nodeID = value; }

    explicit Value(date_t value) : dataType(DATE) { this->val.dateVal = value; }

    explicit Value(timestamp_t value) : dataType(TIMESTAMP) { this->val.timestampVal = value; }

    explicit Value(interval_t value) : dataType(INTERVAL) { this->val.intervalVal = value; }

    Value& operator=(const Value& other);

    string toString() const;

public:
    union Val {
        constexpr Val() : booleanVal{false} {}
        bool booleanVal;
        int64_t int64Val;
        double doubleVal;
        gf_string_t strVal;
        nodeID_t nodeID;
        date_t dateVal;
        timestamp_t timestampVal;
        interval_t intervalVal;
        gf_list_t listVal;
    } val;
    // Note: dataType cannot be UNSTRUCTURED. Any Value has a fixed known data type.
    DataType dataType;
};

} // namespace common
} // namespace graphflow
