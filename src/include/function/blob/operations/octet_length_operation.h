#pragma once

#include "common/types/blob.h"

namespace kuzu {
namespace function {
namespace operation {

struct OctetLength {
    static inline void operation(common::blob_t& input, int64_t& result) {
        result = input.value.len;
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
