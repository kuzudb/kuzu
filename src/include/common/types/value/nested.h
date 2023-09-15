#pragma once

#include <cstdint>

#include "common/api.h"

namespace kuzu {
namespace common {

class Value;

class NestedVal {
public:
    KUZU_API static uint32_t getChildrenSize(const Value* val);

    KUZU_API static Value* getChildVal(const Value* val, uint32_t idx);
};

} // namespace common
} // namespace kuzu
