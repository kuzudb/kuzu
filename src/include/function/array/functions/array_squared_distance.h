#pragma once

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct ArraySquaredDistance {
    template<typename T>
    static inline void operation(common::list_entry_t& left, common::list_entry_t& right, T& result,
        common::ValueVector& leftVector, common::ValueVector& rightVector,
        common::ValueVector& /*resultVector*/) {
        auto leftElements = (T*)common::ListVector::getListValues(&leftVector, left);
        auto rightElements = (T*)common::ListVector::getListValues(&rightVector, right);
        KU_ASSERT(left.size == right.size);
        result = 0;
        for (auto i = 0u; i < left.size; i++) {
            const auto diff = leftElements[i] - rightElements[i];
            result += diff * diff;
        }
    }
};

} // namespace function
} // namespace kuzu
