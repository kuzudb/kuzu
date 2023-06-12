#pragma once

#include "function/map/operations/base_map_extract_operation.h"

namespace kuzu {
namespace function {
namespace operation {

struct MapKeys : public BaseMapExtract {
    static void operation(common::list_entry_t& listEntry, common::list_entry_t& resultEntry,
        common::ValueVector& listVector, common::ValueVector& resultVector) {
        auto mapKeyVector = common::MapVector::getKeyVector(&listVector);
        auto mapKeyValues = common::MapVector::getMapKeys(&listVector, listEntry);
        BaseMapExtract::operation(
            resultEntry, resultVector, mapKeyValues, mapKeyVector, listEntry.size);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
