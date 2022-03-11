#pragma once

#include "types.h"

namespace graphflow {
namespace common {

struct gf_list_t {

public:
    gf_list_t() : childType{INVALID}, capacity{0}, size{0}, overflowPtr{nullptr} {}

    void copyOverflow(const gf_list_t& other);

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
