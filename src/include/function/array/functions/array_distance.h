#pragma once

#include "math.h"

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct ArrayDistance {
    template<typename T>
    static inline void operation(common::list_entry_t& left, common::list_entry_t& right, T& result,
        common::ValueVector& leftVector, common::ValueVector& rightVector,
        common::ValueVector& /*resultVector*/) {
        auto leftElements = (T*)common::ListVector::getListValues(&leftVector, left);
        auto rightElements = (T*)common::ListVector::getListValues(&rightVector, right);
        result = 0;
        for (auto i = 0u; i < left.size; i++) {
            auto diff = leftElements[i] - rightElements[i];
            result += diff * diff;
        }
        result = std::sqrt(result);
    }
};

} // namespace function
} // namespace kuzu
