#pragma once

#include <cassert>

#include "src/common/include/types.h"

namespace graphflow {
namespace common {

class gf_list_t {
    friend class ValueVector;
    friend class OverflowBuffer;

public:
    gf_list_t() : childType{INVALID}, capacity{0}, size{0}, overflowPtr{nullptr} {}

    void copyOverflow(const gf_list_t& other) {
        memcpy(overflowPtr, other.overflowPtr, size * TypeUtils::getDataTypeSize(childType));
    }

    string toString() const;

private:
    string elementToString(uint64_t pos) const;

public:
    DataType childType;
    uint64_t capacity;
    uint64_t size;
    uint8_t* overflowPtr;
};

} // namespace common
} // namespace graphflow
