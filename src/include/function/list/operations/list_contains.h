#pragma once

#include <cassert>
#include <cstring>

#include "common/types/ku_list.h"
#include "list_position_operation.h"

namespace kuzu {
namespace function {
namespace operation {

struct ListContains {
    template<typename T>
    static inline void operation(common::ku_list_t& list, T& element, uint8_t& result,
        const common::DataType& leftDataType, const common::DataType& rightDataType) {
        int64_t pos;
        ListPosition::operation(list, element, pos, leftDataType, rightDataType);
        result = (pos != 0);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
