#pragma once

#include "common/api.h"
#include "common/types/ku_string.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace function {

struct Concat {
    template<class A, class B, class R>
    static inline void operation(
        A& /*left*/, B& /*right*/, R& /*result*/, common::ValueVector& /*valueVector*/) {
        KU_UNREACHABLE;
    }

    KUZU_API static void concat(const char* left, uint32_t leftLen, const char* right,
        uint32_t rightLen, common::ku_string_t& result, common::ValueVector& resultValueVector);
};

template<>
inline void Concat::operation(common::ku_string_t& left, common::ku_string_t& right,
    common::ku_string_t& result, common::ValueVector& resultValueVector) {
    concat((const char*)left.getData(), left.len, (const char*)right.getData(), right.len, result,
        resultValueVector);
}

template<>
inline void Concat::operation(std::string& left, std::string& right, common::ku_string_t& result,
    common::ValueVector& resultValueVector) {
    concat(left.c_str(), left.length(), right.c_str(), right.length(), result, resultValueVector);
}

} // namespace function
} // namespace kuzu
