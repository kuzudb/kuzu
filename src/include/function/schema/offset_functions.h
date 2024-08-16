#pragma once

#include "common/types/types.h"

namespace kuzu {
namespace function {

struct Offset {

    static void operation(common::internalID_t& input, int64_t& result) { result = input.offset; }
};

} // namespace function
} // namespace kuzu
