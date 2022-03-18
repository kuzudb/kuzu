#pragma once

#include "types.h"

namespace graphflow {
namespace common {

struct gf_list_t {

public:
    gf_list_t() : childType{INVALID}, size{0}, overflowPtr{0} {}

    void set(const uint8_t* values) const;

private:
    friend class TypeUtils;

    void set(const gf_list_t& other);
    void set(DataType childType, const vector<uint8_t*>& parameters);

public:
    DataType childType;
    uint64_t size;
    uint64_t overflowPtr;
};

} // namespace common
} // namespace graphflow
