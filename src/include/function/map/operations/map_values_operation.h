#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {
namespace operation {

struct MapValues : public BaseMapExtract {
    static void operation(common::list_entry_t& listEntry, common::list_entry_t& resultEntry,
        common::ValueVector& listVector, common::ValueVector& resultVector) {
        auto mapValueVector = common::MapVector::getValueVector(&listVector);
        auto mapValueValues = common::MapVector::getMapValues(&listVector, listEntry);
        BaseMapExtract::operation(
            resultEntry, resultVector, mapValueValues, mapValueVector, listEntry.size);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
