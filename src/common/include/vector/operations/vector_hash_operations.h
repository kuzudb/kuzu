#pragma once

#include "src/common/include/vector/value_vector.h"

namespace graphflow {
namespace common {

struct VectorHashOperations {
    static void Hash(ValueVector& operand, ValueVector& result);
};

} // namespace common
} // namespace graphflow
