#pragma once

#include "src/common/include/types.h"

using namespace std;

namespace graphflow {
namespace common {

class Literal {
public:
    Literal() {}

    Literal(const Literal& value) { *this = value; }

    explicit Literal(uint8_t value) : dataType(BOOL) { this->val.booleanVal = value; }

    explicit Literal(int64_t value) : dataType(INT64) { this->val.int64Val = value; }

    explicit Literal(double value) : dataType(DOUBLE) { this->val.doubleVal = value; }

    explicit Literal(date_t value) : dataType(DATE) { this->val.dateVal = value; }

    explicit Literal(const string& value) : dataType(STRING) { this->strVal = value; }

    Literal& operator=(const Literal& other);

    string toString() const;

public:
    union Val {
        uint8_t booleanVal;
        int64_t int64Val;
        double doubleVal;
        nodeID_t nodeID;
        date_t dateVal;
    } val{};

    string strVal;

    DataType dataType;
};
} // namespace common
} // namespace graphflow
