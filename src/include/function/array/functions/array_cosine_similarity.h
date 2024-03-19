#pragma once

#include "math.h"

#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct ArrayCosineSimilarity {
    template<typename T>
    static inline void operation(common::list_entry_t& left, common::list_entry_t& right, T& result,
        common::ValueVector& leftVector, common::ValueVector& rightVector,
        common::ValueVector& /*resultVector*/) {
        auto leftElements = (T*)common::ListVector::getListValues(&leftVector, left);
        auto rightElements = (T*)common::ListVector::getListValues(&rightVector, right);
        T distance = 0;
        T normLeft = 0;
        T normRight = 0;
        for (auto i = 0u; i < left.size; i++) {
            auto x = leftElements[i];
            auto y = rightElements[i];
            distance += x * y;
            normLeft += x * x;
            normRight += y * y;
        }
        auto similarity = distance / (std::sqrt(normLeft) * std::sqrt(normRight));
        result = std::max(static_cast<T>(-1), std::min(similarity, static_cast<T>(1)));
    }
};

} // namespace function
} // namespace kuzu
