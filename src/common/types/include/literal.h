#pragma once

#include "types_include.h"

using namespace std;

namespace graphflow {
namespace common {

class Literal {

public:
    Literal() : dataType{INVALID} {}

    explicit Literal(DataType dataType) : dataType{dataType} {}

    Literal(const Literal& other);

    explicit Literal(bool value) : dataType(BOOL) { this->val.booleanVal = value; }

    explicit Literal(int64_t value) : dataType(INT64) { this->val.int64Val = value; }

    explicit Literal(double value) : dataType(DOUBLE) { this->val.doubleVal = value; }

    explicit Literal(date_t value) : dataType(DATE) { this->val.dateVal = value; }

    explicit Literal(timestamp_t value) : dataType(TIMESTAMP) { this->val.timestampVal = value; }

    explicit Literal(interval_t value) : dataType(INTERVAL) { this->val.intervalVal = value; }

    explicit Literal(const string& value) : dataType(STRING) { this->strVal = value; }

public:
    union Val {
        bool booleanVal;
        int64_t int64Val;
        double doubleVal;
        nodeID_t nodeID;
        date_t dateVal;
        timestamp_t timestampVal;
        interval_t intervalVal;
    } val{};

    string strVal;
    vector<Literal> listVal;

    DataType dataType;
};
} // namespace common
} // namespace graphflow
