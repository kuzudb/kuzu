#pragma once

#include <cstdint>
#include <string>

#include "src/common/include/types.h"

using namespace std;

namespace graphflow {
namespace common {

class Value {
public:
    Value() = default;

    explicit Value(DataType dataType) : dataType(dataType) {}

    explicit Value(uint8_t value) : dataType(BOOL) { this->primitive.boolean_ = value; }

    explicit Value(int32_t value) : dataType(INT32) { this->primitive.integer_ = value; }

    explicit Value(double value) : dataType(DOUBLE) { this->primitive.double_ = value; }

    explicit Value(const string& value) : dataType(STRING) { this->str = value; }

    inline void setBool(bool value) { this->primitive.boolean_ = value; }

    inline void setInt(int32_t value) { this->primitive.integer_ = value; }

    inline void setDouble(double value) { this->primitive.double_ = value; }

    inline void setString(string value) { this->str = std::move(value); }

    inline void setNodeID(nodeID_t value) { this->nodeID = value; }

    string toString() const;

public:
    union PrimitiveValue {
        uint8_t boolean_;
        int32_t integer_;
        double double_;
    } primitive;

    string str;
    nodeID_t nodeID;

public:
    DataType dataType;
};

} // namespace common
} // namespace graphflow
