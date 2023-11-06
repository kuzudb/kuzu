#pragma once

#include "common/vector/value_vector.h"
#include "function/comparison/comparison_functions.h"

namespace kuzu {
namespace function {

struct MapExtract {
    template<typename T>
    static void operation(common::list_entry_t& listEntry, T& key,
        common::list_entry_t& resultEntry, common::ValueVector& listVector,
        common::ValueVector& keyVector, common::ValueVector& resultVector) {
        auto mapKeyVector = common::MapVector::getKeyVector(&listVector);
        auto mapKeyValues = common::MapVector::getMapKeys(&listVector, listEntry);
        auto mapValVector = common::MapVector::getValueVector(&listVector);
        auto mapValPos = listEntry.offset;
        uint8_t comparisonResult;
        for (auto i = 0u; i < listEntry.size; i++) {
            Equals::operation(*reinterpret_cast<T*>(mapKeyValues), key, comparisonResult,
                mapKeyVector, &keyVector);
            if (comparisonResult) {
                resultEntry = common::ListVector::addList(&resultVector, 1 /* size */);
                common::ListVector::getDataVector(&resultVector)
                    ->copyFromVectorData(resultEntry.offset, mapValVector, mapValPos);
                return;
            }
            mapKeyValues += mapKeyVector->getNumBytesPerValue();
            mapValPos++;
        }
        // If the key is not found, return an empty list.
        resultEntry = common::ListVector::addList(&resultVector, 0 /* size */);
    }
};

} // namespace function
} // namespace kuzu
