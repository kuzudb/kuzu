#pragma once

#include <cstdint>
#include <string>

#include "types_include.h"

using namespace std;

namespace kuzu {
namespace common {

class Value {
public:
    Value() : dataType{ANY} {};

    Value(const Value& value) { *this = value; }

    // TODO(Guodong): this interface is weird. Consider remove.
    explicit Value(DataType dataType) : dataType{move(dataType)} {}

    explicit Value(bool value) : dataType(BOOL) { this->val.booleanVal = value; }

    explicit Value(int64_t value) : dataType(INT64) { this->val.int64Val = value; }

    explicit Value(double value) : dataType(DOUBLE) { this->val.doubleVal = value; }

    explicit Value(const string& value) : dataType(STRING) { this->val.strVal.set(value); }

    explicit Value(nodeID_t value) : dataType(NODE_ID) { this->val.nodeID = value; }

    explicit Value(date_t value) : dataType(DATE) { this->val.dateVal = value; }

    explicit Value(timestamp_t value) : dataType(TIMESTAMP) { this->val.timestampVal = value; }

    explicit Value(interval_t value) : dataType(INTERVAL) { this->val.intervalVal = value; }

    Value& operator=(const Value& other);

public:
    union Val {
        constexpr Val() : booleanVal{false} {}
        bool booleanVal;
        int64_t int64Val;
        double doubleVal;
        ku_string_t strVal;
        nodeID_t nodeID;
        date_t dateVal;
        timestamp_t timestampVal;
        interval_t intervalVal;
        ku_list_t listVal;
    } val;
    // Note: dataType cannot be UNSTRUCTURED. Any Value has a fixed known data type.
    DataType dataType;
};

} // namespace common
} // namespace kuzu
