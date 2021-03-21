#pragma once

#include <cstdint>
#include <string>

#include "src/common/include/types.h"

using namespace std;

namespace graphflow {
namespace common {

class Literal {
public:
    Literal() = default;

    explicit Literal(DataType type) : type(type) {}

    explicit Literal(uint8_t value) : type(BOOL) { this->primitive.boolean = value; }

    explicit Literal(int32_t value) : type(INT32) { this->primitive.integer = value; }

    explicit Literal(double value) : type(DOUBLE) { this->primitive.double_ = value; }

    explicit Literal(const string& str) : type(STRING) { this->str = str; }

    union PrimitiveValue {
        uint8_t boolean;
        int32_t integer;
        double double_;
    } primitive;

    string str;
    nodeID_t nodeID;

    inline void setBool(bool val) { this->primitive.boolean = val; }

    inline void setInt(int32_t val) { this->primitive.integer = val; }

    inline void setDouble(double val) { this->primitive.double_ = val; }

    inline void setString(string val) { this->str = std::move(val); }

    inline void setNodeID(nodeID_t val) { this->nodeID = val; }

    string toString() const;

private:
    DataType type;
};

} // namespace common
} // namespace graphflow
