#pragma once

#include "common/vector/value_vector.h"
#include "common/vector/value_vector_utils.h"

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
        for (auto i = 0u; i < listEntry.size; i++) {
            if (common::TypeUtils::isValueEqual(
                    *reinterpret_cast<T*>(mapKeyValues), key, mapKeyVector, &keyVector)) {
                resultEntry = common::ListVector::addList(&resultVector, 1 /* size */);
                common::ValueVectorUtils::copyValue(
                    common::ListVector::getListValues(&resultVector, resultEntry),
                    *common::ListVector::getDataVector(&resultVector), mapValValues, *mapValVector);
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
