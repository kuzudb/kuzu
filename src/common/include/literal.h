#pragma once

#include "src/common/include/types.h"

using namespace std;

namespace graphflow {
namespace common {

class Literal {
public:
    Literal(const Literal& value) { *this = value; }

    explicit Literal(uint8_t value) : dataType(BOOL) { this->val.booleanVal = value; }

    explicit Literal(int32_t value) : dataType(INT32) { this->val.int32Val = value; }

    explicit Literal(double value) : dataType(DOUBLE) { this->val.doubleVal = value; }

    explicit Literal(const string& value) : dataType(STRING) { this->strVal = value; }

    Literal& operator=(const Literal& other);

    string toString() const;

public:
    union Val {
        uint8_t booleanVal;
        int32_t int32Val;
        double doubleVal;
        nodeID_t nodeID;
    } val{};

    string strVal;

    DataType dataType;
};
} // namespace common
} // namespace graphflow
