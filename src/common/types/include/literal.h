#pragma once

#include <utility>

#include "types_include.h"

using namespace std;

namespace graphflow {
namespace common {

class Literal {

public:
    Literal() : dataType{INVALID} {}

    // TODO(Guodong): initializing literal with only datatype doesn't make sense to me. Consider
    // remove this interface.
    explicit Literal(DataType dataType) : dataType{move(dataType)} {}

    Literal(const Literal& other);

    explicit Literal(bool value) : dataType(BOOL) { this->val.booleanVal = value; }

    explicit Literal(int64_t value) : dataType(INT64) { this->val.int64Val = value; }

    explicit Literal(double value) : dataType(DOUBLE) { this->val.doubleVal = value; }

    explicit Literal(date_t value) : dataType(DATE) { this->val.dateVal = value; }

    explicit Literal(timestamp_t value) : dataType(TIMESTAMP) { this->val.timestampVal = value; }

    explicit Literal(interval_t value) : dataType(INTERVAL) { this->val.intervalVal = value; }

    explicit Literal(const string& value) : dataType(STRING) { this->strVal = value; }

    void bind(const Literal& other);

    template<typename T>
    static Literal createLiteral(T value) {
        assert(false);
    }

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

template<>
inline Literal Literal::createLiteral(bool value) {
    return Literal(value);
}

template<>
inline Literal Literal::createLiteral(int64_t value) {
    return Literal(value);
}

template<>
inline Literal Literal::createLiteral(double_t value) {
    return Literal(value);
}

template<>
inline Literal Literal::createLiteral(date_t value) {
    return Literal(value);
}

template<>
inline Literal Literal::createLiteral(timestamp_t value) {
    return Literal(value);
}

template<>
inline Literal Literal::createLiteral(interval_t value) {
    return Literal(value);
}

template<>
inline Literal Literal::createLiteral(const char* value) {
    return Literal(string(value));
}

template<>
inline Literal Literal::createLiteral(string value) {
    return Literal(value);
}

} // namespace common
} // namespace graphflow
