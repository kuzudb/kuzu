#pragma once

#include "common/vector/value_vector.h"
#include "list_unique_function.h"

namespace kuzu {
namespace function {

struct ListDistinct {
    static inline void operation(common::list_entry_t& input, common::list_entry_t& result,
        common::ValueVector& inputVector, common::ValueVector& resultVector) {
        auto numUniqueValues = ListUnique::appendListElementsToValueSet(input, inputVector);
        result = common::ListVector::addList(&resultVector, numUniqueValues);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        auto resultDataVectorBuffer =
            common::ListVector::getListValuesWithOffset(&resultVector, result, 0 /* offset */);
        ListUnique::appendListElementsToValueSet(input, inputVector, nullptr,
            [&resultDataVector, &resultDataVectorBuffer](common::ValueVector& dataVector,
                uint64_t pos) -> void {
                resultDataVector->copyFromVectorData(resultDataVectorBuffer, &dataVector,
                    dataVector.getData() + pos * dataVector.getNumBytesPerValue());
                resultDataVectorBuffer += dataVector.getNumBytesPerValue();
            });
    }
};

} // namespace function
} // namespace kuzu
