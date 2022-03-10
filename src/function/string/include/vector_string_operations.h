#pragma once

#include "src/common/include/vector/value_vector.h"

using namespace graphflow::common;

namespace graphflow {
namespace function {

struct VectorStringOperations {

    static void Concat(ValueVector& left, ValueVector& right, ValueVector& result);

    static void Contains(ValueVector& left, ValueVector& right, ValueVector& result);
    static uint64_t ContainsSelect(ValueVector& left, ValueVector& right, sel_t* selectedPositions);

    static void StartsWith(ValueVector& left, ValueVector& right, ValueVector& result);
    static uint64_t StartsWithSelect(
        ValueVector& left, ValueVector& right, sel_t* selectedPositions);
};

} // namespace function
} // namespace graphflow
