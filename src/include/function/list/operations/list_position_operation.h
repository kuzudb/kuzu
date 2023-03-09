#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_list.h"

namespace kuzu {
namespace function {
namespace operation {

struct ListPosition {
    // Note: this function takes in a 1-based pos (The index of the first element in the list
    // is 1).
    template<typename T>
    static inline void operation(common::ku_list_t& list, T& pos, int64_t& result,
        const common::DataType& leftDataType, const common::DataType& rightDataType) {
        if (*leftDataType.childType != rightDataType) {
            result = 0;
            return;
        }
        auto values = reinterpret_cast<T*>(list.overflowPtr);
        for (auto i = 0u; i < list.size; i++) {
            if (common::TypeUtils::isValueEqual(
                    values[i], pos, *leftDataType.childType, rightDataType)) {
                result = i + 1;
                return;
            }
        }
        result = 0;
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
