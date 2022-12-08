#pragma once

#include "types.h"

namespace kuzu {
namespace common {

struct ku_list_t {

public:
    ku_list_t() : size{0}, overflowPtr{0} {}
    ku_list_t(uint64_t size, uint64_t overflowPtr) : size{size}, overflowPtr{overflowPtr} {}

    void set(const uint8_t* values, const DataType& dataType) const;

private:
    friend class InMemOverflowBufferUtils;

    void set(const std::vector<uint8_t*>& parameters, DataTypeID childTypeId);

public:
    uint64_t size;
    uint64_t overflowPtr;
};

} // namespace common
} // namespace kuzu
