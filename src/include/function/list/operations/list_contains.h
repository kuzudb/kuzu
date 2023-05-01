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
    static inline void operation(common::list_entry_t& list, T& element, uint8_t& result,
        common::ValueVector& listVector, common::ValueVector& elementVector,
        common::ValueVector& resultVector) {
        int64_t pos;
        ListPosition::operation(list, element, pos, listVector, elementVector, resultVector);
        result = (pos != 0);
    }
};

} // namespace operation
} // namespace function
} // namespace kuzu
