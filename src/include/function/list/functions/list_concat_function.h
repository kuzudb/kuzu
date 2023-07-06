#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_list.h"

namespace kuzu {
namespace function {

struct ListConcat {
public:
    static inline void operation(common::list_entry_t& left, common::list_entry_t& right,
        common::list_entry_t& result, common::ValueVector& leftVector,
        common::ValueVector& rightVector, common::ValueVector& resultVector) {
        result = common::ListVector::addList(&resultVector, left.size + right.size);
        auto resultDataVector = common::ListVector::getDataVector(&resultVector);
        auto resultPos = result.offset;
        auto leftDataVector = common::ListVector::getDataVector(&leftVector);
        auto leftPos = left.offset;
        for (auto i = 0u; i < left.size; i++) {
            resultDataVector->copyFromVectorData(resultPos++, leftDataVector, leftPos++);
        }
        auto rightDataVector = common::ListVector::getDataVector(&rightVector);
        auto rightPos = right.offset;
        for (auto i = 0u; i < right.size; i++) {
            resultDataVector->copyFromVectorData(resultPos++, rightDataVector, rightPos++);
        }
    }
};

} // namespace function
} // namespace kuzu
