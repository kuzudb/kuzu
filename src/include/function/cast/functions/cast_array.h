#pragma once

#include "common/types/types.h"
#include "common/vector/value_vector.h"

using namespace kuzu::common;

namespace kuzu {
namespace function {

struct CastArrayHelper {
    static bool checkCompatibleNestedTypes(LogicalTypeID sourceTypeID, LogicalTypeID targetTypeID);

    static bool containsListToArray(const LogicalType& srcType, const LogicalType& dstType);

    static void validateListEntry(ValueVector* inputVector, const LogicalType& resultType,
        uint64_t pos);
};

} // namespace function
} // namespace kuzu
