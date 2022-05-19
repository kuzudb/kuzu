#pragma once

#include "types.h"

namespace graphflow {
namespace common {

struct gf_list_t {

public:
    gf_list_t() : size{0}, overflowPtr{0} {}

    void set(const uint8_t* values, const DataType& dataType) const;

private:
    friend class TypeUtils;

    void set(const vector<uint8_t*>& parameters, DataTypeID childTypeId);

public:
    uint64_t size;
    uint64_t overflowPtr;
};

} // namespace common
} // namespace graphflow
