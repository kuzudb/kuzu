#pragma once

#include "common/types/blob.h"

namespace kuzu {
namespace function {
namespace operation {

struct Encode {
    static inline void operation(
        common::ku_string_t& input, common::blob_t& result, common::ValueVector& resultVector) {
        common::StringVector::addString(&resultVector, result.value, input);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
