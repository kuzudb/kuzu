#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_list.h"

namespace kuzu {
namespace function {
namespace operation {

struct ListConcat {
public:
    static inline void operation(common::list_entry_t& left, common::list_entry_t& right,
        common::list_entry_t& result, common::ValueVector& leftVector,
        common::ValueVector& rightVector, common::ValueVector& resultVector) {
        result = common::ListVector::addList(&resultVector, left.size + right.size);
        auto leftValues = common::ListVector::getListValues(&leftVector, left);
        auto leftDataVector = common::ListVector::getDataVector(&leftVector);
        auto resultValues = common::ListVector::getListValues(&resultVector, result);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        auto numBytesPerValue = resultDataVector->getNumBytesPerValue();
        for (auto i = 0u; i < left.size; i++) {
            resultDataVector->copyFromVectorData(resultValues, leftDataVector, leftValues);
            resultValues += numBytesPerValue;
            leftValues += numBytesPerValue;
        }
        auto rightValues = common::ListVector::getListValues(&rightVector, right);
        auto rightDataVector = common::ListVector::getDataVector(&rightVector);
        for (auto i = 0u; i < right.size; i++) {
            resultDataVector->copyFromVectorData(resultValues, rightDataVector, rightValues);
            resultValues += numBytesPerValue;
            rightValues += numBytesPerValue;
        }
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
