#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {
namespace operation {

struct MapExtract {
    template<typename T>
    static void operation(common::list_entry_t& listEntry, T& key,
        common::list_entry_t& resultEntry, common::ValueVector& listVector,
        common::ValueVector& keyVector, common::ValueVector& resultVector) {
        auto mapKeyVector = common::MapVector::getKeyVector(&listVector);
        auto mapKeyValues = common::MapVector::getMapKeys(&listVector, listEntry);
        auto mapValVector = common::MapVector::getValueVector(&listVector);
        auto mapValValues = common::MapVector::getMapValues(&listVector, listEntry);
        uint8_t comparisonResult;
        for (auto i = 0u; i < listEntry.size; i++) {
            Equals::operation(*reinterpret_cast<T*>(mapKeyValues), key, comparisonResult,
                mapKeyVector, &keyVector);
            if (comparisonResult) {
                resultEntry = common::ListVector::addList(&resultVector, 1 /* size */);
                common::ListVector::getDataVector(&resultVector)
                    ->copyFromVectorData(
                        common::ListVector::getListValues(&resultVector, resultEntry), mapValVector,
                        mapValValues);
                return;
            }
            mapKeyValues += mapKeyVector->getNumBytesPerValue();
            mapValValues += mapValVector->getNumBytesPerValue();
        }
        // If the key is not found, return an empty list.
        resultEntry = common::ListVector::addList(&resultVector, 0 /* size */);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
