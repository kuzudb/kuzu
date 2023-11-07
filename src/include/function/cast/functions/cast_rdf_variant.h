#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct CastFromRdfVariant {
    template<typename T>
    static void operation(common::ValueVector& inputVector, uint64_t inputPos, T& result,
        common::ValueVector& resultVector, uint64_t resultPos);
};

} // namespace function
} // namespace kuzu
