#pragma once

#include "math.h"

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

// Euclidean distance between two arrays.
// TODO: This should be extended to support more distance functions.
struct ArrayDistance {
    template<typename T>
    static inline void operation(common::list_entry_t& left, common::list_entry_t& right, T& result,
        common::ValueVector& leftVector, common::ValueVector& rightVector,
        common::ValueVector& /*resultVector*/) {
        auto leftElements = (T*)common::ListVector::getListValues(&leftVector, left);
        auto rightElements = (T*)common::ListVector::getListValues(&rightVector, right);
        KU_ASSERT(left.size == right.size);
        operation(leftElements, rightElements, left.size, result);
    }

    template<typename T>
    static inline void operation(const T* left, const T* right, common::length_t size, T& result) {
        result = 0;
        for (auto i = 0u; i < size; i++) {
            auto diff = left[i] - right[i];
            result += diff * diff;
        }
        result = std::sqrt(result);
    }
};

} // namespace function
} // namespace kuzu
